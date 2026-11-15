# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# Canale per TimStreams
# ------------------------------------------------------------

import binascii
import calendar
import os
import re
import socket
import struct
import sys
import threading
import time
import zlib
from collections import OrderedDict, deque
from datetime import datetime, timedelta

PY3 = sys.version_info[0] >= 3

# Compatibilità Python 2 (Kodi 18 e precedenti) / Python 3 (Kodi 19+)
if PY3:
    import http.server as http_server
    import socketserver
    from urllib.parse import parse_qs, quote, urljoin, urlparse
    _range = range
else:
    import BaseHTTPServer as http_server
    import SocketServer as socketserver
    from urlparse import parse_qs, urljoin, urlparse
    from urllib import quote
    _range = xrange

from core import support, httptools
from platformcode import logger

try:
    import requests
except Exception:
    requests = None

if requests is not None:
    try:
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    except Exception:
        pass


typo = support.typo
host = support.config.get_channel_url()
API = host.rstrip('/') + '/api'
UA = httptools.get_user_agent()
BASE_HEADERS = {
    'User-Agent': UA,
    'Referer': host + '/',
    'Accept': 'application/json, text/html, */*'
}
API_TTL = 60
_CACHE = {}


if PY3:
    def _byte(buf, index):
        return buf[index]
else:
    def _byte(buf, index):
        return ord(buf[index])


# ============================================================
# HTTP (sessioni riutilizzate per thread: meno handshake TLS)
# ============================================================

_LOCAL = threading.local()


def _session():
    session = getattr(_LOCAL, 'session', None)
    if session is None and requests is not None:
        session = requests.Session()
        session.verify = False
        session.headers.update({'User-Agent': UA})
        _LOCAL.session = session
    return session


def _reset_session():
    session = getattr(_LOCAL, 'session', None)
    if session is not None:
        try:
            session.close()
        except Exception:
            pass
    _LOCAL.session = None


def _http_get(url, headers=None, timeout=25, referer=None, retries=1):
    session = _session()
    if session is None:
        raise RuntimeError('requests non disponibile')

    request_headers = {
        'Referer': referer or _PROXY.get('referer') or (host + '/')
    }
    if headers:
        request_headers.update(headers)

    last_error = None
    for attempt in _range(retries + 1):
        try:
            return session.get(url, headers=request_headers, timeout=timeout)
        except Exception as exc:
            last_error = exc
            # Connessione keep-alive scaduta: la ricreiamo e riproviamo una volta.
            _reset_session()
            session = _session()
    raise last_error


# ============================================================
# API
# ============================================================

def _api(path, force=False):
    """Legge un endpoint JSON della API, con una piccola cache."""
    if requests is None:
        logger.error('timstreams: modulo requests non disponibile')
        return {}

    now = time.time()
    cached = _CACHE.get(path)
    if not force and cached and now - cached[0] < API_TTL:
        return cached[1]

    try:
        response = _http_get(API + path, headers=BASE_HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            _CACHE[path] = (now, data)
        return data
    except Exception as exc:
        logger.error('timstreams API %s: %s' % (path, exc))
        return {}


def get_catalog(force=False):
    data = _api('/live-upcoming', force=force)
    events = data.get('events') or []
    genres = data.get('genres') or []
    if not isinstance(events, list):
        events = []
    if not isinstance(genres, list):
        genres = []
    return {'events': events, 'genres': genres}


# ============================================================
# Utility
# ============================================================

# La API restituisce orari senza fuso: il sito li interpreta come
# America/New_York (lo si vede nel bundle JS, costante `America/New_York`).
# Per mostrarli nel fuso del dispositivo passiamo per UTC e poi per l'ora
# locale del sistema. L'ora legale USA vale dal 2007 con queste regole.
_SITE_UTC_OFFSET_STD = -5 * 3600
_SITE_UTC_OFFSET_DST = -4 * 3600
_ISO_RE = re.compile(
    r'^\s*(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})'
    r'(?::(\d{2}))?(?:\.(\d+))?\s*(Z|[+-]\d{2}:?\d{2})?\s*$'
)


def _nth_weekday_day(year, month, weekday, n):
    """Giorno del mese dell'n-esimo weekday (lunedì=0 ... domenica=6)."""
    first = datetime(year, month, 1)
    return 1 + (weekday - first.weekday()) % 7 + 7 * (n - 1)


def _site_offset(value):
    """Offset UTC del sito per un orario locale America/New_York."""
    start = datetime(value.year, 3, _nth_weekday_day(value.year, 3, 6, 2), 2, 0)
    end = datetime(value.year, 11, _nth_weekday_day(value.year, 11, 6, 1), 2, 0)
    if start <= value < end:
        return _SITE_UTC_OFFSET_DST
    return _SITE_UTC_OFFSET_STD


def parse_time(value):
    """Converte l'orario della API in UTC naive, utile per ordinare gli eventi.

    Gli orari senza fuso sono nel fuso del sito (America/New_York); se è
    presente un offset esplicito (Z o +HH:MM) viene usato quello.
    """
    if not value:
        return datetime.max

    match = _ISO_RE.match(str(value))
    if not match:
        return datetime.max

    year, month, day, hour, minute = [int(match.group(i)) for i in range(1, 6)]
    second = int(match.group(6) or 0)
    fraction = match.group(7) or ''
    microsecond = int((fraction + '000000')[:6])

    try:
        parsed = datetime(year, month, day, hour, minute, second, microsecond)
    except (ValueError, TypeError):
        return datetime.max

    zone = match.group(8)
    if zone and zone != 'Z':
        sign = 1 if zone[0] == '+' else -1
        digits = zone[1:].replace(':', '')
        offset = sign * (int(digits[:2]) * 60 + int(digits[2:4])) * 60
        return parsed - timedelta(seconds=offset)

    if zone == 'Z':
        return parsed

    # Nessun fuso: è l'ora locale del sito, la portiamo a UTC.
    return parsed - timedelta(seconds=_site_offset(parsed))


def format_time(value):
    """Mostra l'orario dell'evento nel fuso orario del dispositivo."""
    parsed = parse_time(value)
    if parsed == datetime.max:
        return str(value or '')
    try:
        # Da UTC naive a epoch, poi all'ora locale del sistema.
        epoch = calendar.timegm(parsed.utctimetuple())
        parsed = datetime.fromtimestamp(epoch)
    except Exception:
        pass
    return parsed.strftime('%d/%m %H:%M')


# Nomi dei generi tradotti in italiano; se un genere non è in elenco resta
# il nome originale restituito dalla API.
_GENRE_LABELS = {
    'soccer': 'Calcio',
    'motorsport': 'Motori',
    'fighting': 'Arti marziali',
    'professional wrestling': 'Wrestling',
    'basketball': 'Basket',
    'american football': 'Football americano',
    'baseball': 'Baseball',
    'tennis': 'Tennis',
    'hockey': 'Hockey',
    'darts': 'Freccette',
    'cricket': 'Cricket',
    'cycling': 'Ciclismo',
    'rugby': 'Rugby',
    'live shows': 'Spettacoli dal vivo',
    'others / uncategorized': 'Altro',
    'tba': 'Da definire',
    'sports': 'Sport',
    'entertainment': 'Intrattenimento',
    'cartoons': 'Cartoni animati',
    'movies/cinema': 'Film / Cinema',
    'news/politics': 'Notizie / Politica',
}


def _genre_label(name):
    if not name:
        return ''
    return _GENRE_LABELS.get(name.strip().lower(), name)


def _genre_lookup(catalog=None):
    """Mappa genre_id -> (nome, {sub_genre_id: nome competizione})."""
    if catalog is None:
        catalog = get_catalog()

    lookup = {}
    for genre in catalog.get('genres') or []:
        categories = {}
        for category in genre.get('sub_categories') or []:
            categories[str(category.get('id'))] = category.get('name') or ''
        lookup[str(genre.get('id'))] = (_genre_label(genre.get('name')), categories)
    return lookup


def _event_details(event, lookup):
    """Restituisce (tipo di sport, competizione) per un evento."""
    info = (lookup or {}).get(str(event.get('genre')))
    if not info:
        return '', ''
    genre_name, categories = info
    return genre_name, categories.get(str(event.get('sub_genre')), '')


def _event_item(item, event, lookup=None):
    title = event.get('name') or 'Evento senza nome'
    genre_name, competition = _event_details(event, lookup)

    lines = []
    when = format_time(event.get('time'))
    if when:
        lines.append('[B]%s[/B]' % when)
    if genre_name:
        lines.append('Tipo: %s' % genre_name)
    if competition:
        lines.append('Competizione: %s' % competition)
    description = event.get('description') or ''
    if description:
        lines.append(description)

    return item.clone(
        title=title,
        fulltitle=title,
        plot='\n'.join(lines),
        thumbnail=event.get('logo') or '',
        fanart=event.get('logo') or '',
        kind='event',
        item_id=event.get('url') or '',
        event_time=event.get('time') or '',
        action='streams'
    )


def _channel_item(item, channel, genre_name=''):
    title = channel.get('name') or 'Canale senza nome'
    plot = 'Canale live'
    if genre_name:
        plot += '\nGenere: %s' % _genre_label(genre_name)

    return item.clone(
        title=title,
        fulltitle=title,
        plot=plot,
        thumbnail=channel.get('logo') or '',
        fanart=channel.get('logo') or '',
        kind='channel',
        item_id=channel.get('url') or '',
        action='streams'
    )


def _replay_item(item, replay):
    title = replay.get('name') or 'Replay senza nome'
    plot = 'Replay'
    if replay.get('date'):
        plot += '\nData: %s' % replay.get('date')

    return item.clone(
        title=title,
        fulltitle=title,
        plot=plot,
        thumbnail=replay.get('logo') or '',
        fanart=replay.get('logo') or '',
        kind='replay',
        item_id=replay.get('url') or '',
        action='streams'
    )


# ============================================================
# Menu
# ============================================================

@support.menu
def mainlist(item):
    top = [
        ('Eventi in evidenza', ['', 'featured_events']),
        ('Tutti gli eventi', ['', 'all_events']),
        ('Eventi per sport', ['', 'genres']),
        ('Canali Sport', ['', 'sports_channels']),
        ('Tutti i canali', ['', 'channel_genres']),
        ('Replay', ['', 'replays'])
    ]
    return locals()


def all_events(item):
    logger.debug()
    catalog = get_catalog()
    lookup = _genre_lookup(catalog)
    eventlist = sorted(
        catalog['events'],
        key=lambda event: parse_time(event.get('time'))
    )
    return [_event_item(item, event, lookup) for event in eventlist]


def featured_events(item):
    logger.debug()
    catalog = get_catalog()
    lookup = _genre_lookup(catalog)
    eventlist = [
        event for event in catalog['events']
        if event.get('featured') is True
    ]
    eventlist.sort(key=lambda event: parse_time(event.get('time')))
    return [_event_item(item, event, lookup) for event in eventlist]


def genres(item):
    logger.debug()
    catalog = get_catalog()
    # Mostriamo solo gli sport che hanno almeno un evento in programma:
    # evita voci vuote o duplicate (es. i due generi "TBA").
    used = set(str(event.get('genre')) for event in catalog['events'])

    itemlist = []
    for genre in catalog['genres']:
        if str(genre.get('id')) not in used:
            continue
        itemlist.append(item.clone(
            title=_genre_label(genre.get('name')) or 'Sport sconosciuto',
            genre_id=genre.get('id'),
            action='subgenres'
        ))
    return support.thumb(itemlist, genre=True)


def subgenres(item):
    logger.debug()
    genre_id = item.__dict__.get('genre_id')
    genre = next(
        (g for g in get_catalog()['genres'] if str(g.get('id')) == str(genre_id)),
        None
    )

    if not genre:
        return []

    categories = genre.get('sub_categories') or []
    if not categories:
        return events(item)

    itemlist = [item.clone(
        title='Tutte le competizioni',
        sub_genre_id=None,
        action='events'
    )]
    for category in categories:
        itemlist.append(item.clone(
            title=category.get('name') or 'Competizione sconosciuta',
            sub_genre_id=category.get('id'),
            action='events'
        ))

    return support.thumb(itemlist, genre=True)


def events(item):
    logger.debug()
    genre_id = item.__dict__.get('genre_id')
    sub_id = item.__dict__.get('sub_genre_id')

    catalog = get_catalog()
    lookup = _genre_lookup(catalog)

    eventlist = []
    for event in catalog['events']:
        if genre_id not in ('', None) and str(event.get('genre')) != str(genre_id):
            continue
        if sub_id not in ('', None) and str(event.get('sub_genre')) != str(sub_id):
            continue
        eventlist.append(event)

    eventlist.sort(key=lambda event: parse_time(event.get('time')))
    return [_event_item(item, event, lookup) for event in eventlist]


def channel_genres(item):
    logger.debug()
    data = _api('/channels')
    genres_list = data.get('genres') or []

    itemlist = [item.clone(
        title='Tutti i canali',
        genre_id='',
        action='channels'
    )]

    # Il canale è pensato per lo sport: mettiamo per primo il genere Sports.
    ordered = sorted(
        genres_list,
        key=lambda g: 0 if str(g.get('name', '')).lower() in ('sports', 'sport') else 1
    )
    for genre in ordered:
        itemlist.append(item.clone(
            title=_genre_label(genre.get('name')) or 'Genere sconosciuto',
            genre_id=genre.get('id'),
            action='channels'
        ))

    return support.thumb(itemlist, genre=True)


def sports_channels(item):
    logger.debug()
    data = _api('/channels')
    genres_list = data.get('genres') or []
    sports = next(
        (g for g in genres_list if str(g.get('name', '')).lower() in ('sports', 'sport')),
        None
    )
    if sports:
        return channels(item.clone(genre_id=sports.get('id')))
    return channels(item)


def channels(item):
    logger.debug()
    data = _api('/channels')
    genre_id = item.__dict__.get('genre_id')
    genres_map = {str(g.get('id')): g.get('name') for g in (data.get('genres') or [])}

    itemlist = []
    for channel in data.get('channels') or []:
        if genre_id not in ('', None) and str(channel.get('genre')) != str(genre_id):
            continue
        if not channel.get('streams'):
            continue
        itemlist.append(_channel_item(
            item, channel, genres_map.get(str(channel.get('genre')), '')
        ))

    itemlist.sort(key=lambda it: (it.title or '').lower())
    return itemlist


def replays(item):
    logger.debug()
    itemlist = [
        _replay_item(item, replay)
        for replay in (_api('/replays').get('replays') or [])
    ]
    itemlist.sort(key=lambda it: it.title.lower())
    return itemlist


# ============================================================
# Sorgenti
# ============================================================

def _get_source(kind, item_id):
    if kind == 'event':
        return next(
            (e for e in get_catalog()['events'] if str(e.get('url')) == str(item_id)),
            None
        )
    if kind == 'channel':
        return next(
            (c for c in (_api('/channels').get('channels') or [])
             if str(c.get('url')) == str(item_id)),
            None
        )
    if kind == 'replay':
        return next(
            (r for r in (_api('/replays').get('replays') or [])
             if str(r.get('url')) == str(item_id)),
            None
        )
    return None


def streams(item):
    logger.debug()
    kind = item.__dict__.get('kind') or ''
    item_id = item.__dict__.get('item_id') or ''
    source = _get_source(kind, item_id)

    if not source:
        logger.error('timstreams: sorgente non trovata (%s %s)' % (kind, item_id))
        return []

    itemlist = []
    for index, stream in enumerate(source.get('streams') or []):
        url = stream.get('url') or ''
        if not url:
            continue
        # Le sorgenti VIP richiedono un account: non sono riproducibili senza login.
        if stream.get('vip'):
            continue
        name = stream.get('name') or 'Sorgente %d' % (index + 1)
        itemlist.append(item.clone(
            title=name,
            url=url,
            stream_name=name,
            kind=kind,
            item_id=item_id,
            action='findvideos'
        ))

    if not itemlist:
        logger.info('timstreams: nessuna sorgente libera disponibile')
    return itemlist


# ============================================================
# Risoluzione del player
# ============================================================

def _decode_player_js(html):
    """
    Il player è protetto da uno script offuscato che costruisce il codice con eval.
    Lo script contiene un array di numeri e due chiavi XOR; ricostruiamo il codice
    per leggere il ticketUrl.
    """
    scripts = re.findall(r'<script[^>]*>([\s\S]*?)</script>', html, re.I)
    for script in scripts:
        if 'window["ev"+"al"]' not in script and "window['ev'+'al']" not in script:
            continue
        array_match = re.search(r'var\s+(_\w+)\s*=\s*\[([0-9,\s]+)\]', script)
        keys_match = re.search(
            r',\s*_\w+\s*=\s*(\d+)\s*,\s*_\w+\s*=\s*(\d+)\s*,', script
        )
        if not array_match or not keys_match:
            continue
        try:
            numbers = ''.join(array_match.group(2).split())
            values = [int(value) for value in numbers.split(',') if value]
            key = int(keys_match.group(1))
            subtract = int(keys_match.group(2))
            decoded = ''.join(
                chr(((value ^ key) - subtract + 256) % 256) for value in values
            )
            if 'ticketUrl' in decoded:
                return decoded
        except Exception:
            continue
    return ''


def _unpack_ticket(data, ticket):
    """Decodifica il ticket XOR restituito dal player."""
    if not data:
        return ''
    token = ticket.rsplit('/', 1)[-1]
    if not token:
        return ''
    raw = bytearray(data)
    token_len = len(token)
    first = raw[0]
    output = bytearray()
    for index in _range(1, len(raw)):
        output.append(
            raw[index]
            ^ ord(token[(index - 1) % token_len])
            ^ ((first + index - 1) & 255)
        )
    return output.decode('utf-8', 'replace').strip()


def _extract_media_playlist(url, referer):
    """
    Segue un eventuale master playlist fino alla playlist multimediale.
    Restituisce (url_playlist, testo).
    """
    text = _http_get(url, referer=referer).text
    for _ in _range(3):
        if '#EXT-X-STREAM-INF' not in text and '#EXT-X-MEDIA:' not in text:
            return url, text
        variant = next(
            (line.strip() for line in text.splitlines()
             if line.strip() and not line.strip().startswith('#')),
            ''
        )
        if not variant:
            return url, text
        url = urljoin(url, variant)
        text = _http_get(url, referer=referer).text
    return url, text


def _resolve_stream(stream_url):
    """
    Apre la pagina del player, recupera il ticket e lo decodifica.
    Restituisce (url_playlist_media, url_pagina_stream).
    """
    response = _http_get(stream_url, headers={'Referer': host + '/'})
    response.raise_for_status()
    html = response.text
    stream_page = response.url

    player_js = _decode_player_js(html)
    ticket_match = re.search(
        r'"ticketUrl"\s*:\s*"([^"]+)"', player_js or html
    )
    if not ticket_match:
        raise ValueError('ticketUrl non trovato')

    ticket = urljoin(stream_page, ticket_match.group(1).replace('\\/', '/'))
    ticket_response = _http_get(ticket, headers={'Referer': stream_page})
    ticket_response.raise_for_status()

    master_url = _unpack_ticket(ticket_response.content, ticket)
    if not master_url.startswith('http'):
        raise ValueError('ticket non valido')

    media_url, _ = _extract_media_playlist(master_url, stream_page)
    return media_url, stream_page


# ============================================================
# Decodifica dei segmenti PNG -> MPEG-TS
# ============================================================

# Se sul dispositivo è disponibile Pillow (C) lo usiamo: la decodifica PNG
# passa da ~0.5s a pochi millisecondi. Altrimenti usiamo il decoder interno.
_PIL_IMAGE = None
_PIL_CHECKED = False


def _pil_image():
    global _PIL_IMAGE, _PIL_CHECKED
    if not _PIL_CHECKED:
        _PIL_CHECKED = True
        try:
            from PIL import Image
            _PIL_IMAGE = Image
        except Exception:
            _PIL_IMAGE = None
    return _PIL_IMAGE


def _pil_payload(Image, data):
    try:
        from io import BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO

    image = Image.open(BytesIO(data))
    try:
        tobytes = getattr(image, 'tobytes', None)
        if tobytes is None:
            tobytes = image.tostring
        return tobytes()
    finally:
        try:
            image.close()
        except Exception:
            pass


def _unfilter_png(data, width, height, bpp):
    """Annulla i filtri PNG riga per riga (solo Python: niente dipendenze)."""
    stride = width * bpp
    output = bytearray(height * stride)
    previous = None
    position = 0
    rng = _range

    for row_index in rng(height):
        filter_type = _byte(data, position)
        position += 1
        row = bytearray(data[position:position + stride])
        position += stride
        offset = row_index * stride

        if filter_type == 0:
            output[offset:offset + stride] = row
            previous = row

        elif filter_type == 1:
            dest = row
            for index in rng(bpp, stride):
                dest[index] = (dest[index] + dest[index - bpp]) & 255
            output[offset:offset + stride] = dest
            previous = dest

        elif filter_type == 2:
            dest = row
            if previous is not None:
                dest = bytearray(
                    (value + up) & 255 for value, up in zip(dest, previous)
                )
            output[offset:offset + stride] = dest
            previous = dest

        elif filter_type == 3:
            dest = row
            if previous is None:
                previous = bytearray(stride)
            for index in rng(min(bpp, stride)):
                dest[index] = (dest[index] + (previous[index] >> 1)) & 255
            for index in rng(bpp, stride):
                dest[index] = (
                    dest[index] + ((dest[index - bpp] + previous[index]) >> 1)
                ) & 255
            output[offset:offset + stride] = dest
            previous = dest

        elif filter_type == 4:
            dest = row
            if previous is None:
                previous = bytearray(stride)
            for index in rng(min(bpp, stride)):
                dest[index] = (dest[index] + previous[index]) & 255
            for index in rng(bpp, stride):
                left = dest[index - bpp]
                up = previous[index]
                up_left = previous[index - bpp]
                estimate = left + up - up_left
                dist_left = abs(estimate - left)
                dist_up = abs(estimate - up)
                dist_up_left = abs(estimate - up_left)
                if dist_left <= dist_up and dist_left <= dist_up_left:
                    predictor = left
                elif dist_up <= dist_up_left:
                    predictor = up
                else:
                    predictor = up_left
                dest[index] = (dest[index] + predictor) & 255
            output[offset:offset + stride] = dest
            previous = dest

        else:
            raise ValueError('filtro PNG non supportato: %d' % filter_type)

    return bytes(output)


def decode_segment(data):
    """Converte un segmento PNG (KWP1) in MPEG-TS; se non è PNG lo lascia com'è."""
    if data[:4] != b'\x89PNG':
        return data

    position = 8
    width = height = bit_depth = color_type = interlace = 0
    kwln = -1
    idat = []

    while position + 12 <= len(data):
        length = struct.unpack('>I', data[position:position + 4])[0]
        chunk_type = data[position + 4:position + 8]
        start = position + 8
        end = start + length

        if chunk_type == b'IHDR':
            width, height, bit_depth, color_type = struct.unpack(
                '>IIBB', data[start:start + 10]
            )
            interlace = _byte(data, start + 12)
        elif chunk_type == b'kwLn' and length == 4:
            kwln = struct.unpack('>I', data[start:start + 4])[0]
        elif chunk_type == b'IDAT':
            idat.append(data[start:end])
        elif chunk_type == b'IEND':
            break

        position = end + 4

    if bit_depth != 8 or interlace != 0:
        return data

    bpp = {0: 1, 2: 3, 4: 2, 6: 4}.get(color_type)
    if not bpp:
        return data

    payload = None
    Image = _pil_image()
    if Image is not None:
        try:
            payload = _pil_payload(Image, data)
            if not isinstance(payload, (bytes, bytearray)):
                payload = None
        except Exception as exc:
            logger.info('timstreams: Pillow non utilizzabile (%s), uso decoder interno' % exc)
            payload = None

    if payload is None:
        try:
            filtered = zlib.decompress(b''.join(idat))
        except Exception as exc:
            logger.error('timstreams PNG zlib: %s' % exc)
            return data

        try:
            payload = _unfilter_png(filtered, width, height, bpp)
        except Exception as exc:
            logger.error('timstreams PNG unfilter: %s' % exc)
            return data

    if color_type in (4, 6):
        keep = 1 if color_type == 4 else 3
        if keep == 1:
            # Scala di grigi + alpha: teniamo solo la luminanza.
            payload = payload[0::2]
        else:
            # RGBA -> RGB, togliendo il canale alpha con slicing (C-level).
            try:
                stripped = bytearray((len(payload) // bpp) * keep)
                stripped[0::3] = payload[0::4]
                stripped[1::3] = payload[1::4]
                stripped[2::3] = payload[2::4]
                payload = bytes(stripped)
            except (TypeError, ValueError):
                # Fallback per interpreti che non supportano gli slice estesi
                # sulle bytearray: molto più lento ma sempre corretto.
                payload = bytes().join(
                    payload[index:index + keep]
                    for index in _range(0, len(payload), bpp)
                )

    if 0 <= kwln <= len(payload):
        payload = payload[:kwln]

    if len(payload) >= 4 and payload[:4] == b'KWP1':
        data_length = struct.unpack('>I', payload[4:8])[0]
        if 8 + data_length <= len(payload):
            payload = payload[8:8 + data_length]
    elif payload and _byte(payload, 0) == 0x47:
        end = (len(payload) // 188) * 188
        while end >= 376 and (
            _byte(payload, end - 188) != 0x47
            or _byte(payload, end - 376) != 0x47
        ):
            end -= 188
        if end > 0:
            payload = payload[:end]

    return payload


# ============================================================
# Proxy HTTP locale (playlist + segmenti decodificati)
# ============================================================

_PROXY_LOCK = threading.Lock()
_REFRESH_LOCK = threading.Lock()
_SEGMENT_LOCK = threading.Lock()
_SEGMENT_CACHE = OrderedDict()
_SEGMENT_CACHE_MAX = 6
_SEGMENT_EVENTS = {}

# Precaricamento: decodifica in anticipo i prossimi segmenti, così il player
# non deve aspettare la decodifica PNG ad ogni richiesta.
_PREFETCH_COND = threading.Condition()
_PREFETCH_QUEUE = deque()
_PREFETCH_THREAD = None
_PREFETCH_MAX = 3

_PROXY = {
    'server': None,
    'port': 0,
    'sid': '',
    'media_url': '',
    'stream_page': '',
    'referer': host + '/'
}


def _random_sid():
    sid = binascii.hexlify(os.urandom(8))
    return sid.decode('ascii') if PY3 else sid


def _rewrite_playlist(text, base_url, prefix):
    text = re.sub(r'RESOLUTION=\d+(?!x),', '', text)
    is_master = '#EXT-X-STREAM-INF' in text or '#EXT-X-MEDIA:' in text
    output = []

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        if stripped.startswith('#'):
            def replace_uri(match):
                uri = match.group(2)
                if not uri or uri.startswith('data:'):
                    return match.group(0)
                absolute = urljoin(base_url, uri)
                is_playlist = is_master and (
                    stripped.startswith('#EXT-X-MEDIA')
                    or stripped.startswith('#EXT-X-I-FRAME-STREAM-INF')
                    or uri.lower().split('?')[0].endswith('.m3u8')
                )
                endpoint = 'variant.m3u8' if is_playlist else 'resource'
                return (
                    match.group(1) + prefix + endpoint + '?u='
                    + quote(absolute.encode('utf-8'), safe='') + match.group(3)
                )

            output.append(re.sub(r'(URI=")([^"]+)(")', replace_uri, line))
        else:
            absolute = urljoin(base_url, stripped)
            endpoint = 'variant.m3u8' if is_master else 'segment.ts'
            output.append(
                prefix + endpoint + '?u='
                + quote(absolute.encode('utf-8'), safe='')
            )

    return '\n'.join(output) + '\n'


def _fetch_playlist(url):
    """Scarica una playlist; se la sessione è scaduta la rigenera e riprova."""
    response = _http_get(url)
    if response.status_code in (403, 404, 410):
        _refresh_stream()
        url = _PROXY['media_url']
        response = _http_get(url)
    response.raise_for_status()
    return response.text, url


def _refresh_stream():
    with _REFRESH_LOCK:
        stream_page = _PROXY.get('stream_page') or ''
        if not stream_page:
            return
        media_url, referer = _resolve_stream(stream_page)
        _PROXY['media_url'] = media_url
        _PROXY['referer'] = referer or _PROXY['referer']
        logger.info('timstreams: URL stream aggiornato')


def _cache_touch(url):
    """Segna un segmento come usato di recente.

    OrderedDict.move_to_end non esiste in Python 2.7 (Kodi 18), quindi
    riposizioniamo la chiave a mano. Va chiamata con _SEGMENT_LOCK acquisito.
    """
    try:
        segment = _SEGMENT_CACHE.pop(url)
    except KeyError:
        return
    _SEGMENT_CACHE[url] = segment


def _download_segment(url):
    response = _http_get(url)
    response.raise_for_status()
    segment = decode_segment(response.content)

    with _SEGMENT_LOCK:
        _SEGMENT_CACHE[url] = segment
        while len(_SEGMENT_CACHE) > _SEGMENT_CACHE_MAX:
            _SEGMENT_CACHE.popitem(last=False)

    return segment


def _get_segment(url):
    with _SEGMENT_LOCK:
        cached = _SEGMENT_CACHE.get(url)
        if cached is not None:
            _cache_touch(url)
            return cached
        event = _SEGMENT_EVENTS.get(url)
        if event is None:
            event = threading.Event()
            _SEGMENT_EVENTS[url] = event
            owner = True
        else:
            owner = False

    if not owner:
        # Un altro thread (proxy o precaricamento) sta già decodificando lo
        # stesso segmento: aspettiamo invece di decodificarlo due volte.
        event.wait(60)
        with _SEGMENT_LOCK:
            cached = _SEGMENT_CACHE.get(url)
            if cached is not None:
                _cache_touch(url)
                return cached
        return _download_segment(url)

    try:
        return _download_segment(url)
    finally:
        with _SEGMENT_LOCK:
            _SEGMENT_EVENTS.pop(url, None)
        event.set()


def _prefetch_worker():
    while True:
        with _PREFETCH_COND:
            while not _PREFETCH_QUEUE:
                _PREFETCH_COND.wait()
            url = _PREFETCH_QUEUE.popleft()
        try:
            _get_segment(url)
        except Exception:
            pass


def _prefetch_segments(segment_urls):
    global _PREFETCH_THREAD

    if not segment_urls:
        return

    candidates = []
    with _SEGMENT_LOCK:
        for url in segment_urls:
            if not url or url in candidates:
                continue
            if url in _SEGMENT_CACHE or url in _SEGMENT_EVENTS:
                continue
            candidates.append(url)

    if not candidates:
        return

    if _PREFETCH_THREAD is None or not _PREFETCH_THREAD.is_alive():
        thread = threading.Thread(target=_prefetch_worker)
        thread.daemon = True
        thread.start()
        _PREFETCH_THREAD = thread

    with _PREFETCH_COND:
        while _PREFETCH_QUEUE:
            _PREFETCH_QUEUE.popleft()
        _PREFETCH_QUEUE.extend(candidates[-_PREFETCH_MAX:])
        _PREFETCH_COND.notify()


class _ProxyServer(socketserver.ThreadingMixIn, http_server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        # Connessioni chiuse dal player: non sono errori da loggare.
        if isinstance(sys.exc_info()[1], (socket.error, socket.timeout)):
            return
        socketserver.BaseServer.handle_error(self, request, client_address)


class _ProxyHandler(http_server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    timeout = 30

    def do_GET(self):
        self._serve(True)

    def do_HEAD(self):
        self._serve(False)

    def _serve(self, with_body):
        parsed = urlparse(self.path)
        prefix = '/p/%s/' % _PROXY.get('sid', '')
        if not _PROXY.get('sid') or not parsed.path.startswith(prefix):
            self.send_error(404)
            return

        route = parsed.path[len(prefix):]
        params = parse_qs(parsed.query)
        upstream = params.get('u', [''])[0]

        if route == 'playlist.m3u8' and not upstream:
            upstream = _PROXY.get('media_url') or ''
        if not upstream:
            self.send_error(404)
            return

        try:
            if route in ('playlist.m3u8', 'variant.m3u8'):
                text, used_url = _fetch_playlist(upstream)
                try:
                    # Precarica i segmenti della playlist multimediale.
                    if '#EXT-X-STREAM-INF' not in text:
                        segment_urls = [
                            urljoin(used_url, line.strip())
                            for line in text.splitlines()
                            if line.strip() and not line.strip().startswith('#')
                        ]
                        _prefetch_segments(segment_urls)
                except Exception:
                    pass
                data = _rewrite_playlist(text, used_url, prefix).encode('utf-8')
                content_type = 'application/vnd.apple.mpegurl'

            elif route == 'segment.ts':
                data = _get_segment(upstream)
                content_type = 'video/mp2t'

            elif route == 'resource':
                response = _http_get(upstream)
                response.raise_for_status()
                data = response.content
                content_type = str(
                    response.headers.get('Content-Type')
                    or 'application/octet-stream'
                )

            else:
                self.send_error(404)
                return

        except Exception as exc:
            logger.error('timstreams proxy %s: %s' % (route, exc))
            self.send_error(502)
            return

        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.end_headers()

        if with_body:
            try:
                self.wfile.write(data)
            except (socket.error, socket.timeout):
                pass

    def log_message(self, *args):
        pass


def _start_proxy(media_url, stream_page, source_url=''):
    with _PROXY_LOCK:
        _PROXY['media_url'] = media_url
        _PROXY['stream_page'] = source_url or stream_page
        _PROXY['referer'] = stream_page or (host + '/')
        _PROXY['sid'] = _random_sid()

        with _SEGMENT_LOCK:
            _SEGMENT_CACHE.clear()

        with _PREFETCH_COND:
            while _PREFETCH_QUEUE:
                _PREFETCH_QUEUE.popleft()

        if _PROXY['server'] is None:
            server = _ProxyServer(('127.0.0.1', 0), _ProxyHandler)
            _PROXY['port'] = server.server_address[1]
            _PROXY['server'] = server
            thread = threading.Thread(target=server.serve_forever)
            thread.daemon = True
            thread.start()
            logger.info('timstreams proxy avviato su 127.0.0.1:%d' % _PROXY['port'])

        return _PROXY['port'], _PROXY['sid']


# ============================================================
# Play
# ============================================================

def findvideos(item):
    logger.debug()

    stream_url = item.url
    if not stream_url:
        logger.error('timstreams: URL stream vuoto')
        return []

    try:
        media_url, stream_page = _resolve_stream(stream_url)
    except Exception as exc:
        logger.error('timstreams: estrazione non riuscita (%s)' % exc)
        # Non è un player TimStreams: lasciamo risolvere l'URL ai resolver
        # già presenti nell'addon (es. Voe per i replay).
        fallback = item.clone(url=stream_url, action='play', server='')
        return support.server(
            fallback, itemlist=[fallback],
            Download=False, Videolibrary=False
        )

    port, sid = _start_proxy(media_url, stream_page, stream_url)
    local_url = 'http://127.0.0.1:%d/p/%s/playlist.m3u8' % (port, sid)
    logger.info('timstreams: riproduzione da %s' % local_url)

    play_item = item.clone(
        server='directo',
        url=local_url,
        manifest='hls',
        action='play',
        referer=stream_page,
        title=item.title
    )

    return support.server(
        play_item, itemlist=[play_item],
        Download=False, Videolibrary=False, CheckLinks=False
    )
