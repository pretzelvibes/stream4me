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

# Il dominio principale arriva da channels.json; timstreams.co.rw è il dominio
# di riserva e serve la stessa API con gli stessi contenuti. Se il principale
# non risponde (down, bloccato dall'ISP o certificato non valido) si passa
# automaticamente all'altro, senza chiedere nulla all'utente.
_PRIMARY_HOST = support.config.get_channel_url().rstrip('/')
_BACKUP_HOST = 'https://timstreams.co.rw'
_HOSTS = []
for _candidate in (_PRIMARY_HOST, _BACKUP_HOST):
    if _candidate and _candidate not in _HOSTS:
        _HOSTS.append(_candidate)

host = _PRIMARY_HOST
API = host + '/api'
UA = httptools.get_user_agent()
BASE_HEADERS = {
    'User-Agent': UA,
    'Referer': host + '/',
    'Accept': 'application/json, text/html, */*'
}
API_TTL = 60
_CACHE = {}

# Dopo un failover si riprova il dominio principale ogni tanto, così si torna
# a usarlo quando guarisce senza pagare un timeout a ogni richiesta.
_PRIMARY_RETRY = 300.0
_primary_retry_at = 0.0


def _set_active_host(candidate):
    """Rende attivo il dominio che ha risposto, così i referer lo seguono."""
    global host, API, _primary_retry_at

    candidate = (candidate or '').rstrip('/')
    if not candidate:
        return
    if candidate != _PRIMARY_HOST:
        # Anche se il backup era già attivo: il controllo del principale è
        # appena fallito e va rimandato, altrimenti lo si ritenterebbe a ogni
        # richiesta pagando un timeout.
        _primary_retry_at = time.time() + _PRIMARY_RETRY
    if candidate == host:
        return
    logger.info('timstreams: dominio attivo %s' % candidate)
    host = candidate
    API = host + '/api'
    BASE_HEADERS['Referer'] = host + '/'


def _api_hosts():
    """Domini da provare, a partire da quello attivo."""
    ordered = [host]
    retry_primary = (host != _PRIMARY_HOST
                     and time.time() >= _primary_retry_at)
    if retry_primary:
        # È passato abbastanza tempo: riproviamo il principale, che è la
        # scelta preferita quando è raggiungibile.
        ordered.insert(0, _PRIMARY_HOST)
    for candidate in _HOSTS:
        if candidate in ordered:
            continue
        if candidate == _PRIMARY_HOST and not retry_primary:
            # Dopo un failover il principale resta in pausa fino allo scadere
            # del timer, altrimenti ogni richiesta pagherebbe un timeout.
            continue
        ordered.append(candidate)
    return ordered


def _site_url(url):
    """Riscrive i link del sito (loghi) sul dominio attivo.

    Kodi scarica le immagini con il proprio stack di rete, quindi l'override
    DNS del canale non le copre: se il principale è bloccato, un logo assoluto
    su tim-streams.com non si vedrebbe. I due domini servono gli stessi file.
    """
    if not url:
        return url
    for base in _HOSTS:
        if url == base or url.startswith(base + '/'):
            return host + url[len(base):]
    return url


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

_DNS_LOCK = threading.Lock()
_DNS_NOTICE = False


def _install_dns_override():
    """Attiva l'override DNS via DoH anche per le sessioni requests locali.

    TimStreams non passa da httptools, che è il punto in cui l'addon monta
    l'adapter responsabile dell'override: senza questa chiamata API, pagina,
    ticket e segmenti userebbero il DNS dell'ISP e i domini bloccati non si
    aprirebbero. La patch è globale e idempotente, quindi vale per tutte le
    richieste del processo; la si ripete alla creazione di ogni sessione così
    un cambio dell'impostazione ha effetto senza riavviare Kodi.
    """
    global _DNS_NOTICE

    with _DNS_LOCK:
        try:
            from core import resolverdns
            resolverdns.install_dns_override()
        except Exception as exc:
            if not _DNS_NOTICE:
                _DNS_NOTICE = True
                logger.info('timstreams: override DNS non disponibile (%s)' % exc)


def _session():
    session = getattr(_LOCAL, 'session', None)
    if session is None and requests is not None:
        _install_dns_override()
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


def _flush_dns(url):
    """Dimentica l'IP DoH del host dopo un errore di connessione.

    CipherSuiteAdapter fa lo stesso per gli altri canali: la cache DNS dura
    un'ora e, se l'IP risolto non è più raggiungibile, senza questo flush
    ogni nuovo tentativo riproverebbe lo stesso indirizzo.
    """
    try:
        hostname = urlparse(url).hostname
    except Exception:
        return
    if not hostname:
        return
    try:
        from core import resolverdns
        resolverdns.flush(hostname)
    except Exception:
        pass


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
            # Un errore di connessione può dipendere da un IP DoH non più
            # valido: lo togliamo dalla cache prima di riprovare.
            if (requests is not None and
                    isinstance(exc, requests.exceptions.ConnectionError)):
                _flush_dns(url)
            # Connessione keep-alive scaduta: la ricreiamo e riproviamo una volta.
            _reset_session()
            session = _session()
    raise last_error


# ============================================================
# API
# ============================================================

def _api(path, force=False):
    """Legge un endpoint JSON della API, con una piccola cache.

    Prova il dominio attivo e, se non risponde, quello di riserva: il canale
    resta utilizzabile anche quando il principale è bloccato o spento.
    """
    if requests is None:
        logger.error('timstreams: modulo requests non disponibile')
        return {}

    now = time.time()
    cached = _CACHE.get(path)
    if not force and cached and now - cached[0] < API_TTL:
        return cached[1]

    for base in _api_hosts():
        headers = dict(BASE_HEADERS)
        headers['Referer'] = base + '/'
        try:
            response = _http_get(base + '/api' + path, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            logger.error('timstreams API %s (%s): %s' % (path, base, exc))
            continue
        if not isinstance(data, dict) or not data:
            logger.error('timstreams API %s (%s): risposta vuota' % (path, base))
            continue
        _set_active_host(base)
        _CACHE[path] = (now, data)
        return data
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
        thumbnail=_site_url(event.get('logo')) or '',
        fanart=_site_url(event.get('logo')) or '',
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
        thumbnail=_site_url(channel.get('logo')) or '',
        fanart=_site_url(channel.get('logo')) or '',
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
        thumbnail=_site_url(replay.get('logo')) or '',
        fanart=_site_url(replay.get('logo')) or '',
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
    text = _http_get(url, referer=referer, timeout=(5, 15)).text
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
        text = _http_get(url, referer=referer, timeout=(5, 15)).text
    return url, text


def _resolve_stream(stream_url):
    """
    Apre la pagina del player, recupera il ticket e lo decodifica.
    Restituisce (url_playlist_media, url_pagina_stream, url_ticket).
    """
    response = _http_get(stream_url, headers={'Referer': host + '/'},
                         timeout=(5, 15))
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
    ticket_response = _http_get(ticket, headers={'Referer': stream_page},
                                timeout=(5, 15))
    ticket_response.raise_for_status()

    master_url = _unpack_ticket(ticket_response.content, ticket)
    if not master_url.startswith('http'):
        raise ValueError('ticket non valido')

    media_url, _ = _extract_media_playlist(master_url, stream_page)
    return media_url, stream_page, ticket


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
# La playlist locale accumula 6-10 segmenti (circa 30-40 s). La cache non deve
# mai sfrattare un segmento ancora in lista: oltre questa soglia vengono
# eliminati solo quelli già usciti dalla playlist (vedi _download_segment).
_SEGMENT_CACHE_MAX = 8
_SEGMENT_EVENTS = {}
# URL attualmente presenti nella playlist servita al player.
_LIVE_URLS = set()

# Precaricamento: decodifica in anticipo i prossimi segmenti, così il player
# non deve aspettare download e decodifica PNG ad ogni richiesta.
# Due thread permettono di sovrapporre rete e decodifica: mentre uno decodifica
# un segmento l'altro sta già scaricando il successivo (utile sui dispositivi
# lenti, dove la sola decodifica Python può pesare centinaia di millisecondi).
_PREFETCH_COND = threading.Condition()
_PREFETCH_QUEUE = deque()
_PREFETCH_THREADS = []
_PREFETCH_WORKERS = 2
_PREFETCH_MAX = 6

_PROXY = {
    'server': None,
    'port': 0,
    'sid': '',
    'media_url': '',
    'stream_page': '',
    'referer': host + '/'
}

# Playlist live servita dalla cache locale.
#
# Ogni ticket produce una playlist "fotografia" che non avanza mai: la stessa
# URL resta ferma anche per minuti e il player, dopo aver consumato i 4
# segmenti (15-24 s), si ferma. La finestra avanza solo chiedendo un nuovo
# ticket, che costa 1-4 s ed è molto più economico della risoluzione completa
# (pagina + JS + ticket: 10-30 s). Le finestre successive si sovrappongono
# (verificato sui PTS), quindi il flusso resta continuo.
#
# Qui l'ultima playlist buona viene servita subito al player e un thread la
# rinnova ogni pochi secondi chiedendo un nuovo ticket. La risoluzione
# completa si fa solo se il ticket smette di funzionare.
#
# I segmenti delle finestre successive vengono accumulati invece di essere
# sostituiti: inputstream.adaptive ha una latenza live di 16 s, più lunga di
# una finestra da 15 s. Con la sola finestra corrente il player non trova il
# punto da cui ripartire e a ogni salto di sequenza svuota il buffer; con
# 30-40 s di segmenti ha sempre un punto valido e assorbe i vuoti del server.
_LIVE_LOCK = threading.Lock()
_LIVE = {
    'text': '',            # ultima playlist multimediale valida (originale)
    'base_url': '',        # URL per risolvere i segmenti relativi
    'sequence': -1,        # #EXT-X-MEDIA-SEQUENCE dell'ultima playlist
    'segments': [],        # [(sequenza, durata, url)] accumulati
    'emit_last': -1,       # sequenza dell'ultimo segmento pubblicato
    'fetched_at': 0.0,     # ultima lettura riuscita
    'changed_at': 0.0,     # ultima volta che la finestra è avanzata
    'target': 4.0,         # #EXT-X-TARGETDURATION della playlist
    'last_try': 0.0,       # ultimo tentativo (anche fallito)
    'last_request': 0.0,   # ultima richiesta del player
    'started_at': 0.0,     # avvio dello stream corrente
    'failures': 0,         # errori consecutivi
    'busy': False,         # un aggiornamento è in corso
    'full_busy': False,    # una risoluzione completa è in corso
    'priming': False,      # primo riempimento in corso
    'primed': threading.Event(),
    'last_full': 0.0,      # ultima risoluzione completa
    'generation': 0,       # cambia ad ogni nuovo stream
    'thread': None,
    'thread_gen': -1
}

# Il ticket della sessione corrente. Va riusato per ottenere finestre nuove:
# è la via più rapida e non richiede di rieseguire il player JS.
_TICKET_LOCK = threading.Lock()
_TICKET = {'url': '', 'referer': ''}

_LIVE_TIMEOUT = (4, 8)     # (connect, read): niente attese di 25 secondi
_TICKET_TIMEOUT = (5, 12)  # il ticket a volte impiega 2-4 s
_LIVE_INTERVAL = 0.5       # passo del thread di aggiornamento
_LIVE_REFRESH_EVERY = 2.0  # ogni quanto chiedere una finestra nuova
_LIVE_IDLE = 30.0          # senza richieste del player l'updater si ferma
_LIVE_FULL_EVERY = 6.0     # non riprovare la pagina troppo spesso
_LIVE_PRIME_TRIES = 3
_LIVE_PRIME_WAIT = 25.0


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


def _fetch_playlist_once(url):
    """Legge una playlist senza rifare la risoluzione completa.

    Timeout corti: durante un blocco del server non vogliamo tenere occupato
    un thread per decine di secondi.
    """
    response = _http_get(url, timeout=_LIVE_TIMEOUT, retries=0)
    if response.status_code in (403, 404, 410):
        raise IOError('playlist non disponibile (%s)' % response.status_code)
    response.raise_for_status()
    return response.text, (response.url or url)


def _playlist_entries(text, base_url):
    """[(sequenza, durata, url)] di una playlist multimediale.

    La media-sequence è l'unico identificativo stabile di un segmento: gli URL
    possono cambiare firma fra un ticket e l'altro, la sequenza no.
    """
    if not text or '#EXT-X-STREAM-INF' in text or '#EXT-X-MEDIA:' in text:
        return []
    sequence = _playlist_sequence(text)
    if sequence < 0:
        sequence = 0
    entries = []
    duration = 0.0
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith('#EXTINF:'):
            try:
                duration = float(stripped[8:].split(',')[0])
            except (TypeError, ValueError):
                duration = 0.0
            continue
        if stripped.startswith('#'):
            continue
        entries.append((
            sequence + len(entries),
            duration,
            urljoin(base_url, stripped)
        ))
    return entries


def _playlist_segments(text, base_url):
    """URL assoluti dei segmenti di una playlist multimediale."""
    return [url for _seq, _duration, url in _playlist_entries(text, base_url)]


def _playlist_sequence(text):
    """Numero di sequenza della playlist (-1 se assente)."""
    match = re.search(r'#EXT-X-MEDIA-SEQUENCE:(\d+)', text or '')
    return int(match.group(1)) if match else -1


def _playlist_target(text):
    """Durata nominale di un segmento (#EXT-X-TARGETDURATION)."""
    match = re.search(r'#EXT-X-TARGETDURATION:\s*([0-9.]+)', text or '')
    try:
        return float(match.group(1)) if match else 0.0
    except (TypeError, ValueError):
        return 0.0


def _build_playlist(source_text, segments):
    """Playlist locale con i segmenti accumulati.

    Le sequenze sono rinumerate in modo contiguo: quando il server salta da
    una finestra all'altra (anche di sette posizioni) non devono apparire
    buchi, altrimenti il player crede che manchino dei segmenti.
    """
    lines = ['#EXTM3U']
    for line in (source_text or '').splitlines():
        stripped = line.strip()
        if not stripped or not stripped.startswith('#'):
            continue
        if stripped == '#EXTM3U' or stripped.startswith('#EXTINF'):
            continue
        if stripped.startswith('#EXT-X-MEDIA-SEQUENCE'):
            continue
        if stripped.startswith('#EXT-X-TARGETDURATION'):
            continue
        if stripped.startswith('#EXT-X-ENDLIST'):
            continue
        if stripped.startswith('#EXT-X-PROGRAM-DATE-TIME'):
            continue
        if stripped not in lines:
            lines.append(stripped)

    longest = max([duration for _seq, duration, _url in segments] + [0.0])
    lines.append('#EXT-X-TARGETDURATION:%d' % max(1, int(longest + 0.999)))
    lines.append('#EXT-X-MEDIA-SEQUENCE:%d' % segments[0][0])
    for _seq, duration, url in segments:
        lines.append('#EXTINF:%.3f,' % duration)
        lines.append(url)
    return '\n'.join(lines) + '\n'


def _live_stale_limit():
    """Da quanti secondi una finestra ferma va considerata bloccata.

    Una finestra contiene quattro segmenti: se non avanza per metà della sua
    durata il player rischia di restare a secco. Sui canali (4 s per segmento)
    sono 8 s; sugli eventi (6 s) 12 s.
    """
    with _LIVE_LOCK:
        target = _LIVE['target'] or 4.0
    return max(6.0, 2.0 * target)


def _live_publish(text, base_url):
    """Unisce la nuova finestra ai segmenti già noti e aggiorna la playlist.

    Il server consegna quattro segmenti per volta, che si sovrappongono alla
    finestra precedente: accumulandoli la playlist locale resta più lunga di
    una singola finestra, così il player ha sempre da dove ripartire.
    """
    global _LIVE_URLS

    entries = _playlist_entries(text, base_url)
    now = time.time()
    if not entries:
        return

    with _LIVE_LOCK:
        newest = entries[-1][0]
        if _LIVE['sequence'] >= 0 and newest < _LIVE['sequence']:
            # Una risoluzione in background può finire dopo che il ticket ha
            # già consegnato una finestra più recente: non riavvolgiamo il
            # player con una playlist vecchia. Un salto enorme invece è un
            # riavvio della numerazione lato server e va accettato.
            if _LIVE['sequence'] - newest < 1000:
                return

        known = dict((seq, (duration, url))
                     for seq, duration, url in _LIVE['segments'])
        previous_newest = _LIVE['sequence']
        for seq, duration, url in entries:
            known[seq] = (duration, url)

        # Quante posizioni nuove sono state aggiunte in coda: serve a
        # rinumerare le sequenze senza buchi.
        appended = len([seq for seq, _d, _u in entries if seq > previous_newest])
        if previous_newest < 0:
            emit_last = newest
        else:
            emit_last = _LIVE['emit_last'] + appended

        target = _playlist_target(text) or _LIVE['target'] or 4.0
        window = max(6, min(10, int(round(30.0 / target))))
        ordered = sorted(known)[-window:]
        segments = [(seq, known[seq][0], known[seq][1]) for seq in ordered]
        first_emit = max(0, emit_last - len(segments) + 1)
        emit_last = first_emit + len(segments) - 1
        emitted = [
            (first_emit + index, duration, url)
            for index, (_seq, duration, url) in enumerate(segments)
        ]

        changed = newest != previous_newest
        _LIVE['segments'] = segments
        _LIVE['emit_last'] = emit_last
        _LIVE['text'] = _build_playlist(text, emitted)
        _LIVE['base_url'] = base_url
        _LIVE['sequence'] = newest
        if target > 0:
            _LIVE['target'] = target
        _LIVE['fetched_at'] = now
        if changed:
            _LIVE['changed_at'] = now
        _LIVE['failures'] = 0

    _LIVE_URLS = set(url for _seq, _duration, url in segments)

    # La deduplica per URL evita di riscaricare i segmenti già presenti;
    # quelli vecchi restano in coda solo se sono ancora nella finestra.
    _prefetch_segments([url for _seq, _duration, url in segments])


def _ticket_reset():
    with _TICKET_LOCK:
        _TICKET['url'] = ''
        _TICKET['referer'] = ''


def _refresh_stream(gen=None):
    """Chiede un nuovo ticket e ne segue la playlist.

    Restituisce (testo, url) oppure None se il ticket non ha risposto. Non fa
    mai la risoluzione completa: quella resta a _full_resolve, perché costa
    molto di più e va usata solo quando serve davvero.

    Un errore di rete non invalida il ticket (i timeout del server sono
    frequenti e temporanei); lo si scarta solo se il server risponde 403/404
    oppure se il contenuto non è più decodificabile.
    """
    with _TICKET_LOCK:
        ticket = _TICKET['url']
        referer = _TICKET['referer']
    if not ticket:
        return None

    if gen is not None:
        with _LIVE_LOCK:
            if gen != _LIVE['generation']:
                return None

    try:
        response = _http_get(
            ticket,
            headers={'Referer': referer or (host + '/')},
            timeout=_TICKET_TIMEOUT,
            retries=0
        )
    except Exception:
        return None

    if response.status_code in (403, 404, 410):
        _ticket_reset()
        return None

    try:
        response.raise_for_status()
        master_url = _unpack_ticket(response.content, ticket)
        media_url, text = _extract_media_playlist(
            master_url, referer or (host + '/')
        )
    except Exception:
        _ticket_reset()
        return None

    if not text or '#EXT-X-STREAM-INF' in text:
        return None

    if gen is not None:
        with _LIVE_LOCK:
            if gen != _LIVE['generation']:
                return None
    _PROXY['media_url'] = media_url
    return text, media_url


def _full_resolve(gen):
    """Risoluzione completa dalla pagina del player (lenta, ultima risorsa)."""
    with _REFRESH_LOCK:
        with _LIVE_LOCK:
            if gen != _LIVE['generation']:
                return None
        stream_page = _PROXY.get('stream_page') or ''
        if not stream_page:
            return None

        media_url, referer, ticket = _resolve_stream(stream_page)

        # Se nel frattempo è partito un altro canale, questo ticket non vale.
        with _LIVE_LOCK:
            if gen != _LIVE['generation']:
                return None
        with _TICKET_LOCK:
            _TICKET['url'] = ticket
            _TICKET['referer'] = referer or stream_page

        _PROXY['media_url'] = media_url
        _PROXY['referer'] = referer or _PROXY['referer']
        logger.info('timstreams: URL stream aggiornato')
        return _fetch_playlist_once(media_url)


def _mark_full():
    with _LIVE_LOCK:
        _LIVE['last_full'] = time.time()


def _full_resolve_worker(gen):
    """Risoluzione completa eseguita fuori dal thread di aggiornamento.

    La pagina del player può impiegare anche venti secondi quando il server è
    congestionato. Farla qui evita di bloccare i tentativi via ticket, che
    intanto possono ricominciare a funzionare da soli.
    """
    result = None
    try:
        result = _full_resolve(gen)
    except Exception as exc:
        with _LIVE_LOCK:
            if gen == _LIVE['generation']:
                _LIVE['failures'] += 1
        logger.info('timstreams: risoluzione completa non riuscita (%s)' % exc)
    if result is not None:
        text, base_url = result
        with _LIVE_LOCK:
            stale = gen != _LIVE['generation']
        if not stale:
            _live_publish(text, base_url)
    with _LIVE_LOCK:
        if gen == _LIVE['generation']:
            _LIVE['full_busy'] = False


def _start_full_resolve(gen):
    """Avvia la risoluzione completa se non ce n'è già una in corso."""
    with _LIVE_LOCK:
        if _LIVE['full_busy']:
            return False
        _LIVE['full_busy'] = True
    thread = threading.Thread(target=_full_resolve_worker, args=(gen,))
    thread.daemon = True
    thread.start()
    return True


def _live_update():
    """Un tentativo di aggiornamento, eseguito in background.

    Il player non aspetta mai la rete: se il ticket non risponde si continua
    a servire la playlist precedente. Le finestre si sovrappongono, quindi
    basta chiedere un ticket nuovo ogni pochi secondi per non restare a secco.
    """
    with _LIVE_LOCK:
        if _LIVE['busy']:
            return
        now = time.time()
        if now - _LIVE['last_try'] < _LIVE_REFRESH_EVERY:
            return
        _LIVE['busy'] = True
        _LIVE['last_try'] = now
        gen = _LIVE['generation']
        stale_for = now - (_LIVE['changed_at'] or _LIVE['started_at'] or now)
        can_full = now - _LIVE['last_full'] >= _LIVE_FULL_EVERY

    try:
        result = _refresh_stream(gen)

        with _TICKET_LOCK:
            ticket_gone = not _TICKET['url']
        # La pagina costa circa un secondo quando il server risponde, ma resta
        # l'ultima risorsa: la si tenta quando il ticket è sparito oppure
        # quando la finestra è ferma da metà della sua durata.
        allow_full = can_full and (
            ticket_gone or stale_for >= _live_stale_limit()
        )

        if result is None:
            if allow_full:
                _mark_full()
                _start_full_resolve(gen)
            raise IOError('playlist non disponibile')

        text, base_url = result
        entries = _playlist_entries(text, base_url)
        newest = entries[-1][0] if entries else -1
        with _LIVE_LOCK:
            if newest >= 0:
                unchanged = newest <= _LIVE['sequence']
            else:
                unchanged = text == _LIVE['text']

        # Il ticket risponde ma continua a servire la stessa fotografia:
        # senza una sessione nuova il player resterebbe fermo.
        if unchanged and allow_full:
            _mark_full()
            _start_full_resolve(gen)

        with _LIVE_LOCK:
            stale = gen != _LIVE['generation']
        if not stale:
            _live_publish(text, base_url)

    except Exception as exc:
        with _LIVE_LOCK:
            if gen == _LIVE['generation']:
                _LIVE['failures'] += 1
                failures = _LIVE['failures']
            else:
                failures = 0
        # Durante un blackout il tentativo si ripete: logghiamo solo il primo
        # errore e poi ogni tanto, per non riempire kodi.log.
        if failures == 1 or (failures and failures % 20 == 0):
            logger.info('timstreams: playlist non aggiornata (%s)' % exc)
    finally:
        with _LIVE_LOCK:
            if gen == _LIVE['generation']:
                _LIVE['busy'] = False


def _live_updater(gen):
    """Tiene aggiornata la playlist finché il player continua a richiederla."""
    while True:
        with _LIVE_LOCK:
            if gen != _LIVE['generation']:
                return
            idle = time.time() - _LIVE['last_request']
        if idle > _LIVE_IDLE:
            return
        _live_update()
        time.sleep(_LIVE_INTERVAL)


def _ensure_live_updater():
    with _LIVE_LOCK:
        gen = _LIVE['generation']
        thread = _LIVE.get('thread')
        if (thread is not None and thread.is_alive()
                and _LIVE.get('thread_gen') == gen):
            return
        thread = threading.Thread(target=_live_updater, args=(gen,))
        thread.daemon = True
        _LIVE['thread'] = thread
        _LIVE['thread_gen'] = gen
    thread.start()


def _live_prime():
    """Primo riempimento della playlist, una volta per stream.

    Gli altri chiamanti aspettano l'evento invece di duplicare la richiesta.
    """
    with _LIVE_LOCK:
        if _LIVE['text']:
            return _LIVE['text'], _LIVE['base_url']
        owner = not _LIVE['priming']
        _LIVE['priming'] = True
        primed = _LIVE['primed']

    if not owner:
        primed.wait(_LIVE_PRIME_WAIT)
        with _LIVE_LOCK:
            return _LIVE['text'], _LIVE['base_url']

    text = ''
    base_url = ''
    try:
        url = _PROXY.get('media_url') or ''
        for attempt in _range(_LIVE_PRIME_TRIES):
            try:
                text, base_url = _fetch_playlist_once(url)
                break
            except Exception:
                if attempt + 1 < _LIVE_PRIME_TRIES:
                    time.sleep(1.0)
        if not text:
            # L'URL iniziale non risponde: proviamo il ticket, poi la pagina.
            result = _refresh_stream()
            if result is None:
                with _LIVE_LOCK:
                    gen = _LIVE['generation']
                result = _full_resolve(gen)
            if result is not None:
                text, base_url = result
        if text:
            _live_publish(text, base_url)
        with _LIVE_LOCK:
            return _LIVE['text'], _LIVE['base_url']
    finally:
        with _LIVE_LOCK:
            _LIVE['priming'] = False
        primed.set()


def _live_playlist():
    """Playlist per il player: subito dalla cache, refresh in background."""
    with _LIVE_LOCK:
        _LIVE['last_request'] = time.time()
    _ensure_live_updater()

    text, base_url = _live_prime()
    if not text:
        raise IOError('playlist non disponibile')
    return text, base_url


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


def _cache_segment(url, segment):
    """Salva un segmento decodificato, sfrattando quelli non più in playlist.

    I segmenti ancora elencati non vengono mai eliminati: inputstream.adaptive
    legge con circa 16 secondi di ritardo e potrebbe chiederli da un momento
    all'altro. La memoria resta comunque limitata, perché la playlist contiene
    al massimo otto-dieci segmenti.
    """
    with _SEGMENT_LOCK:
        _SEGMENT_CACHE[url] = segment
        while len(_SEGMENT_CACHE) > _SEGMENT_CACHE_MAX:
            victim = None
            for key in _SEGMENT_CACHE:
                # Né quelli in playlist né quello appena scaricato: il nuovo
                # segmento può non essere ancora stato pubblicato.
                if key not in _LIVE_URLS and key != url:
                    victim = key
                    break
            if victim is None:
                break
            _SEGMENT_CACHE.pop(victim)


def _download_segment(url):
    # Connessione breve e lettura generosa: la lettura è per singolo chunk,
    # quindi una linea lenta ma stabile non viene interrotta, mentre un CDN
    # bloccato non tiene in attesa il player per 50 secondi (25 + retry).
    # In caso di errore il player (o il prossimo aggiornamento) riprova.
    response = _http_get(url, timeout=(5, 20), retries=0)
    response.raise_for_status()
    segment = decode_segment(response.content)
    _cache_segment(url, segment)
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


def _start_prefetch_workers():
    """Avvia (una sola volta) i thread che preparano i segmenti in anticipo."""
    global _PREFETCH_THREADS

    alive = [thread for thread in _PREFETCH_THREADS if thread.is_alive()]
    _PREFETCH_THREADS = alive
    while len(alive) < _PREFETCH_WORKERS:
        thread = threading.Thread(target=_prefetch_worker)
        thread.daemon = True
        thread.start()
        alive.append(thread)
        _PREFETCH_THREADS = alive


def _prefetch_segments(segment_urls):
    """Accoda i segmenti mancanti, nell'ordine in cui il player li chiederà.

    Una playlist HLS elenca i segmenti dal più vecchio al più recente e
    inputstream.adaptive parte dal primo della finestra (la sua latenza live
    predefinita, 16 s, è più lunga dei 15 s della finestra): accodando gli
    ultimi tre si saltava proprio il segmento necessario all'avvio, che finiva
    per essere scaricato solo su richiesta del player, con un'attesa in più.
    """
    if not segment_urls:
        return

    with _SEGMENT_LOCK:
        pending = set(_PREFETCH_QUEUE)
        candidates = []
        for url in segment_urls:
            if not url or url in pending:
                continue
            if url in _SEGMENT_CACHE or url in _SEGMENT_EVENTS:
                continue
            candidates.append(url)
            pending.add(url)

    if not candidates:
        return

    _start_prefetch_workers()

    with _PREFETCH_COND:
        room = _PREFETCH_MAX - len(_PREFETCH_QUEUE)
        if room > 0:
            _PREFETCH_QUEUE.extend(candidates[:room])
        _PREFETCH_COND.notify_all()


def _warm_up_stream():
    """Prepara in anticipo la playlist e i primi segmenti.

    Tra la risoluzione dell'URL e l'apertura del player Kodi impiega circa un
    secondo: lo sfruttiamo per riempire la cache, così il primo segmento è già
    pronto (o quasi) quando inputstream.adaptive lo chiede. Su Firestick e
    Raspberry questo toglie un'attesa iniziale di uno o due secondi.
    """
    def run():
        try:
            text, base_url = _live_prime()
            _prefetch_segments(_playlist_segments(text, base_url))
        except Exception as exc:
            logger.info('timstreams: precaricamento non riuscito (%s)' % exc)

    thread = threading.Thread(target=run)
    thread.daemon = True
    thread.start()


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
    # Il player legge da 127.0.0.1: senza Nagle la playlist arriva subito.
    disable_nagle_algorithm = True

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

        if not upstream and route != 'playlist.m3u8':
            self.send_error(404)
            return

        try:
            if route in ('playlist.m3u8', 'variant.m3u8'):
                if upstream:
                    text, used_url = _fetch_playlist_once(upstream)
                else:
                    # Cache locale: risposta immediata, aggiornamento in background.
                    text, used_url = _live_playlist()
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


def _start_proxy(media_url, stream_page, source_url='', ticket_url=''):
    global _LIVE_URLS

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

        with _TICKET_LOCK:
            _TICKET['url'] = ticket_url or ''
            _TICKET['referer'] = stream_page or (host + '/')

        now = time.time()
        with _LIVE_LOCK:
            old_primed = _LIVE.get('primed')
            _LIVE['text'] = ''
            _LIVE['base_url'] = ''
            _LIVE['sequence'] = -1
            _LIVE['segments'] = []
            _LIVE['emit_last'] = -1
            _LIVE['fetched_at'] = 0.0
            _LIVE['changed_at'] = now
            _LIVE['last_try'] = now
            _LIVE['last_request'] = now
            _LIVE['started_at'] = now
            _LIVE['failures'] = 0
            _LIVE['busy'] = False
            _LIVE['full_busy'] = False
            _LIVE['priming'] = False
            _LIVE['target'] = 4.0
            _LIVE['primed'] = threading.Event()
            _LIVE['last_full'] = 0.0
            _LIVE['generation'] += 1
            _LIVE_URLS = set()
        if old_primed is not None:
            # Sblocca chi stava aspettando il primo riempimento del canale
            # precedente: il suo evento non verrebbe mai impostato.
            old_primed.set()

        _start_prefetch_workers()
        _ensure_live_updater()

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
        media_url, stream_page, ticket_url = _resolve_stream(stream_url)
    except Exception as exc:
        logger.error('timstreams: estrazione non riuscita (%s)' % exc)
        # Non è un player TimStreams: lasciamo risolvere l'URL ai resolver
        # già presenti nell'addon (es. Voe per i replay).
        fallback = item.clone(url=stream_url, action='play', server='')
        return support.server(
            fallback, itemlist=[fallback],
            Download=False, Videolibrary=False
        )

    port, sid = _start_proxy(media_url, stream_page, stream_url, ticket_url)
    _warm_up_stream()
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
