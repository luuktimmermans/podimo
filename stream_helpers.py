import asyncio

from datetime import datetime, timezone

from email.utils import format_datetime, parsedate_to_datetime

import fcntl

import hashlib

import html

import json

import logging

import math

import os

from pathlib import Path

import re

import secrets

import shutil

import signal

import time

from urllib.parse import urlsplit

from uuid import UUID

import xml.etree.ElementTree as ET

def atomic_bytes(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.writing')
    with temporary.open('wb') as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)

def write_json(path, value):
    atomic_bytes(path, json.dumps(value, ensure_ascii=False, indent=2).encode())

def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default

def safe_error(error):
    return re.sub(r'https?://\S+', '[URL verborgen]', str(error))[-1200:]

async def process(*args, timeout=3600):
    child = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(child.communicate(), timeout)
    except BaseException:
        if child.returncode is None:
            child.kill()
        await child.communicate()
        raise
    if child.returncode:
        raise RuntimeError(f'{args[0]}: {safe_error(stderr.decode(errors="replace"))}')
    return stdout

def https_url(url):
    if not isinstance(url, str) or urlsplit(url).scheme != 'https' or not urlsplit(url).hostname:
        raise ValueError('Een geldige HTTPS-bron ontbreekt.')
    return url

async def convert_image(url, directory):
    url = https_url(url)
    name = hashlib.sha256(url.encode()).hexdigest() + '.jpg'
    target = directory / name
    if target.exists() and target.stat().st_size > 0:
        return name
    temporary = target.with_suffix('.part.jpg')
    try:
        await process('ffmpeg', '-nostdin', '-hide_banner', '-loglevel', 'error',
            '-rw_timeout', '30000000', '-protocol_whitelist', 'https,http,tcp,tls',
            '-i', url, '-frames:v', '1', '-q:v', '2', '-y', str(temporary), timeout=120)
        if not temporary.exists() or temporary.stat().st_size == 0:
            raise ValueError('Lege afbeelding.')
        os.replace(temporary, target)
        return name
    finally:
        temporary.unlink(missing_ok=True)

def date_value(raw):
    if not raw:
        raise ValueError('Publicatiedatum ontbreekt.')
    try:
        result = datetime.fromisoformat(str(raw).replace('Z', '+00:00'))
    except ValueError:
        result = parsedate_to_datetime(raw)
    return result if result.tzinfo else result.replace(tzinfo=timezone.utc)

def build_feed(job, podcast, episodes, records, base, cover):
    root = ET.Element('rss', version='2.0')
    ch = ET.SubElement(root, 'channel')
    def node(parent, tag, value):
        ET.SubElement(parent, tag).text = str(value or '')
    urlbase = f'{base}/archive/{job["token"]}'
    title = podcast.get('title') or episodes[0].get('podcastName') or job['podcast_id']
    link = podcast.get('webAddress') or f'https://podimo.com/shows/{job["podcast_id"]}'
    if not str(link).startswith(('http://', 'https://')):
        link = f'https://podimo.com/shows/{job["podcast_id"]}'
    node(ch, 'title', title)
    node(ch, 'link', link)
    node(ch, 'description', podcast.get('description') or title)
    node(ch, 'language', podcast.get('language') or job['locale'])
    node(ch, 'lastBuildDate', format_datetime(datetime.now(timezone.utc)))
    node(ch, 'generator', 'Podimo private complete feed')
    ET.SubElement(ch, f'{{{ATOM}}}link', href=urlbase+'/feed.xml', rel='self', type='application/rss+xml')
    author = podcast.get('authorName') or episodes[0].get('artist')
    if author:
        node(ch, f'{{{ITUNES}}}author', author)
    node(ch, f'{{{ITUNES}}}block', 'yes')
    if cover:
        image = ET.SubElement(ch, 'image')
        node(image, 'url', urlbase+'/'+cover)
        node(image, 'title', title)
        node(image, 'link', link)
        ET.SubElement(ch, f'{{{ITUNES}}}image', href=urlbase+'/'+cover)
    for ep in sorted(episodes, key=lambda e: date_value(e.get('publishDatetime') or e.get('datetime')), reverse=True):
        rec = records[ep['id']]
        item = ET.SubElement(ch, 'item')
        node(item, 'title', ep.get('title') or ep['id'])
        node(item, 'description', ep.get('description') or '')
        node(item, f'{{{CONTENT}}}encoded', ep.get('description') or '')
        ET.SubElement(item, 'guid', isPermaLink='false').text = ep['id']
        node(item, 'pubDate', format_datetime(date_value(ep.get('publishDatetime') or ep.get('datetime'))))
        node(item, f'{{{ITUNES}}}duration', round(rec['duration']))
        if ep.get('artist') or author:
            node(item, f'{{{ITUNES}}}author', ep.get('artist') or author)
        if rec.get('image'):
            ET.SubElement(item, f'{{{ITUNES}}}image', href=urlbase+'/'+rec['image'])
        ET.SubElement(item, 'enclosure', url=urlbase+'/'+ep['id']+'.m4a',
                      length=str(rec['size']), type='audio/mp4')
    return ET.tostring(root, encoding='utf-8', xml_declaration=True)

class PodimoSource:
    def __init__(self, service):
        self.service = service

    async def fetch(self, job):
        import cloudscraper
        s = self.service
        with cloudscraper.create_scraper() as scraper:
            if s.HTTP_PROXY:
                scraper.proxies = {'https': s.HTTP_PROXY}
            client = await s.check_auth(s.PODIMO_EMAIL, s.PODIMO_PASSWORD, job['region'], job['locale'], scraper)
            if not client:
                raise RuntimeError('Podimo-login mislukt.')
            # Force fresh signed URLs, without deleting another process\'s cache.
            try:
                # getPodcasts uses the module-level function; temporarily bypass reads in this worker only.
                import podimo.client as client_module
                original_get = client_module.getCacheEntry
                client_module.getCacheEntry = lambda *args, **kwargs: None
                return await client.getPodcasts(job['podcast_id'], scraper)
            finally:
                client_module.getCacheEntry = original_get

ITUNES = 'http://www.itunes.com/dtds/podcast-1.0.dtd'
CONTENT = 'http://purl.org/rss/1.0/modules/content/'
ATOM = 'http://www.w3.org/2005/Atom'
for prefix, uri in [('itunes', ITUNES), ('content', CONTENT), ('atom', ATOM)]:
    ET.register_namespace(prefix, uri)

