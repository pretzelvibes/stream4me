# -*- coding: utf-8 -*-
"""
Crea lo zip "S4Me-installer.zip" da pubblicare su GitHub Pages.

Lo zip contiene l'addon completo (cartella top-level "plugin.video.s4me/")
piu' il file "last_commit.txt" con lo SHA del commit: cosi' l'updater
integrato (platformcode/updater.py) sa gia' di essere aggiornato e non
riscarica nulla al primo avvio.

Uso:
    python tools/build_installer.py [commit] [-o cartella_output]

Senza argomenti usa HEAD. Va eseguito dalla root del repository, dopo che
le modifiche sono state committate: git archive esporta esattamente i file
del commit (senza .git, __pycache__, last_commit.txt, file non tracciati).
"""
from __future__ import print_function

import argparse
import io
import os
import subprocess
import sys
import zipfile

ADDON_ID = 'plugin.video.s4me'
ZIP_NAME = 'S4Me-installer.zip'
TRACKING_FILE = 'last_commit.txt'


def git(*args):
    out = subprocess.check_output(['git'] + list(args))
    return out.decode('utf-8', 'replace').strip()


def build(commit, output_dir):
    repo_root = git('rev-parse', '--show-toplevel')
    sha = git('rev-parse', commit)

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    target = os.path.join(os.path.abspath(output_dir), ZIP_NAME)

    # 1) archivio git con la cartella top-level attesa da Kodi
    archive = subprocess.Popen(
        ['git', 'archive', '--format=zip', '--prefix=' + ADDON_ID + '/', sha],
        cwd=repo_root, stdout=subprocess.PIPE
    )
    data = archive.communicate()[0]
    if archive.returncode != 0:
        raise SystemExit('git archive non riuscito')

    # 2) aggiungo last_commit.txt (non e' tracciato, ma serve all'updater)
    with zipfile.ZipFile(io.BytesIO(data)) as src:
        names = src.namelist()
        infos = [(info, src.read(info.filename)) for info in src.infolist()]

    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as dst:
        for info, content in infos:
            dst.writestr(info, content)
        dst.writestr(ADDON_ID + '/' + TRACKING_FILE, sha)

    # 3) controlli minimi
    with zipfile.ZipFile(target) as z:
        names = z.namelist()
        addon_xml = ADDON_ID + '/addon.xml'
        if addon_xml not in names:
            raise SystemExit('addon.xml mancante nello zip: ' + addon_xml)
        content = z.read(addon_xml).decode('utf-8', 'replace')
        if 'id="' + ADDON_ID + '"' not in content:
            raise SystemExit('id addon inatteso in addon.xml')
        if z.read(ADDON_ID + '/' + TRACKING_FILE).decode('utf-8') != sha:
            raise SystemExit('last_commit.txt non valido')
        import re
        match = re.search(r'id="' + ADDON_ID + r'"[^>]*version="([^"]+)"', content)
        if not match:
            raise SystemExit('versione non trovata in addon.xml')
        version = match.group(1)

    print('commit:   ' + sha)
    print('versione: ' + version)
    print('file:     ' + str(len(names)))
    print('dimensione: %.1f MB' % (os.path.getsize(target) / 1048576.0))
    print('zip:      ' + target)
    return target


def main():
    parser = argparse.ArgumentParser(description='Crea lo S4Me-installer.zip')
    parser.add_argument('commit', nargs='?', default='HEAD',
                        help='commit da esportare (default: HEAD)')
    parser.add_argument('-o', '--output', default='dist',
                        help='cartella di output (default: dist)')
    args = parser.parse_args()
    build(args.commit, args.output)


if __name__ == '__main__':
    sys.exit(main())
