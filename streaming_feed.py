"""Stable private RSS and playback URLs, with no local audio downloads."""
import asyncio
import fcntl
import hashlib
import html
import os
from pathlib import Path
import re
import secrets
import sys
import threading
import time
from uuid import UUID
import xml.etree.ElementTree as ET

from stream_helpers import (PodimoSource, build_feed, convert_image, https_url,
                            read_json, write_json)


def enabled():
    return os.getenv('STREAMING_FEEDS', 'false').lower() in {'1', 'true', 'yes'}


def ensure_job(cache_dir, podcast_id, region='nl', locale='nl-NL'):
    podcast_id = str(UUID(podcast_id))
    root = Path(cache_dir) / 'stream-feeds'
    root.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(f'{podcast_id}:{region}:{locale}'.encode()).hexdigest()
    with (root/'registry.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        index = root/(key+'.json')
        job = read_json(index)
        if job is None:
            job = dict(podcast_id=podcast_id, region=region, locale=locale,
                       token=secrets.token_hex(32))
            directory = root/job['token']
            directory.mkdir()
            write_json(directory/'job.json', job)
            write_json(index, job)
    return job


def media_for(episode):
    for name in ('audio', 'streamMedia'):
        media = episode.get(name) or {}
        if media.get('url'):
            https_url(media['url'])
            return media
    raise ValueError('Deze aflevering heeft geen audiobron.')


def make_feed(job, data, base):
    episodes = data.get('episodes') or []
    if not episodes:
        raise ValueError('Podimo levert geen afleveringen voor deze podcast.')
    records = {}
    for ep in episodes:
        if str(UUID(ep['id'])) != ep['id']:
            raise ValueError('Ongeldig aflevering-ID.')
        media = media_for(ep)
        records[ep['id']] = {'size': 0, 'duration': float(media.get('duration') or 0),
                              'image': ep['id']+'.jpg' if ep.get('imageUrl') else 'cover.jpg'}
    if len(records) != len(episodes):
        raise ValueError('Dubbele aflevering-ID ontvangen.')
    root = ET.fromstring(build_feed(job, data.get('podcast') or {}, episodes,
                                    records, base, 'cover.jpg'))
    # The endpoint produces real MP3 bytes. Final length is unknown until EOF.
    for enclosure in root.findall('./channel/item/enclosure'):
        enclosure.set('url', enclosure.get('url').replace('.m4a', '.mp3'))
        enclosure.set('type', 'audio/mpeg')
        enclosure.set('length', '0')
    xml = ET.tostring(root, encoding='utf-8', xml_declaration=True)
    return xml.replace(f'{base}/archive/{job["token"]}/'.encode(),
                       f'{base}/stream/{job["token"]}/'.encode())


class Catalog:
    """Serialize legacy client's temporary cache bypass; never block Quart's loop."""
    def __init__(self, service):
        self.source = PodimoSource(service)
        self.lock = threading.Lock()
        self.values = {}

    def fetch_sync(self, job, fresh):
        with self.lock:
            now = time.monotonic()
            cached = self.values.get(job['token'])
            # Playback never uses the hour-old feed cache. The short cache only
            # coalesces simultaneous HEAD and GET calls for fresh source links.
            if cached and now-cached[0] < (15 if fresh else 3600):
                return cached[1]
            result = asyncio.run(self.source.fetch(job))
            self.values[job['token']] = (time.monotonic(), result)
            # Bound memory when many different podcasts have been submitted.
            if len(self.values) > 20:
                oldest = min(self.values, key=lambda k: self.values[k][0])
                self.values.pop(oldest)
            return result

    async def fetch(self, job, fresh=False):
        return await asyncio.to_thread(self.fetch_sync, job, fresh)


def register_routes(app, cache_dir):
    from quart import Response, abort, redirect, request, send_from_directory
    catalog = None
    image_lock = asyncio.Lock()
    audio_slots = asyncio.Semaphore(2)

    async def get_data(job, fresh=False):
        nonlocal catalog
        if catalog is None:
            service = sys.modules[app.import_name]
            if not service.LOCAL_CREDENTIALS:
                raise ValueError('Lokale inloggegevens zijn vereist.')
            catalog = Catalog(service)
        return await catalog.fetch(job, fresh)

    @app.route('/stream/<token>/<filename>')
    async def stream_file(token, filename):
        if not enabled() or not re.fullmatch('[0-9a-f]{64}', token):
            abort(404)
        directory = Path(cache_dir)/'stream-feeds'/token
        job = read_json(directory/'job.json')
        if not job:
            abort(404)
        if filename == 'status':
            body = ('<!doctype html><html lang="nl"><meta charset="utf-8">'
                    '<meta name="viewport" content="width=device-width, initial-scale=1">'
                    '<title>Podimo privéfeed</title><style>body{font:18px system-ui;'
                    'max-width:760px;margin:60px auto;padding:24px;line-height:1.6}</style>'
                    '<h1>Je privéfeed</h1><p>Alle beschikbare afleveringen worden '
                    'opgehaald wanneer je de feed opent. Audio wordt niet op je server bewaard.</p>'
                    f'<p><a href="/stream/{token}/feed.xml">Open de RSS-feed</a></p>'
                    '<p>Voeg die RSS-link als Private toe aan Pocket Casts. '
                    'De eerste keer ophalen kan even duren.</p>'
                    '<p>Deze versie maakt MP3 tijdens het afspelen. Vooruitspoelen buiten de buffer is niet gegarandeerd. Test afspelen '
                    'in Pocket Casts. Bewaar de feed-URL privé.</p></html>')
            return Response(body, mimetype='text/html')
        is_audio = re.fullmatch(r'([0-9a-f-]{36})\.(?:mp3|m3u8)', filename)
        is_image = re.fullmatch(r'(cover|[0-9a-f-]{36})\.jpg', filename)
        if filename != 'feed.xml' and not is_audio and not is_image:
            abort(404)
        try:
            data = await get_data(job, fresh=bool(is_audio))
            service = sys.modules[app.import_name]
            base = f'{service.PODIMO_PROTOCOL}://{service.PODIMO_HOSTNAME}'.rstrip('/')
            if filename == 'feed.xml':
                return Response(make_feed(job, data, base), mimetype='application/rss+xml')
            episodes = data.get('episodes') or []
            if is_audio:
                ep = next((e for e in episodes if e['id'] == is_audio[1]), None)
                if ep is None:
                    abort(404)
                return await mp3_response(media_for(ep)['url'], request.method, audio_slots, Response)
            images = (data.get('podcast') or {}).get('images') or {}
            url = images.get('coverImageUrl') or (episodes[0].get('imageUrl') if episodes else None)
            if is_image[1] != 'cover':
                ep = next((e for e in episodes if e['id'] == is_image[1]), None)
                if ep is None:
                    abort(404)
                url = ep.get('imageUrl') or url
            if not url:
                abort(404)
            # Only artwork is cached. Keep at most ~50 MB of JPEGs per podcast.
            async with image_lock:
                name = await convert_image(url, directory)
                result = await send_from_directory(str(directory), name, mimetype='image/jpeg', conditional=True)
                files = sorted(directory.glob('*.jpg'), key=lambda p: p.stat().st_mtime, reverse=True)
                size = 0
                for path in files:
                    size += path.stat().st_size
                    if size > 50*1024*1024 and path.name != name:
                        path.unlink(missing_ok=True)
                return result
        except (ValueError, RuntimeError, OSError, KeyError):
            # Do not expose source links or login errors to remote RSS clients.
            return Response('Podimo-bron tijdelijk niet beschikbaar. Probeer het later opnieuw.',
                            status=502, headers={'Retry-After':'60'}, mimetype='text/plain')

    @app.after_request
    async def stream_headers(response):
        if request.path.startswith('/stream/'):
            # A permanent/cacheable redirect would preserve expiring CDN URLs.
            response.headers['Cache-Control'] = 'private, no-store'
            response.headers['X-Robots-Tag'] = 'noindex, nofollow'
        return response


async def mp3_response(url, method, slots, Response):
    """Bounded, backpressured MP3 stream. No temporary audio files or fake ranges."""
    headers = {'Accept-Ranges': 'none', 'X-Accel-Buffering': 'no',
               'Content-Disposition': 'inline', 'Cache-Control': 'private, no-store'}
    if method == 'HEAD':
        result = Response(b'', mimetype='audio/mpeg', headers=headers)
        # Empty HEAD body does not mean the resource has length zero.
        result.automatically_set_content_length = False
        result.headers.pop('Content-Length', None)
        return result
    try:
        await asyncio.wait_for(slots.acquire(), timeout=1)
    except asyncio.TimeoutError:
        return Response('Alle audiostreams zijn in gebruik. Probeer opnieuw.', status=503,
                        headers={'Retry-After':'15'})
    child = None
    closed = False
    async def close():
        nonlocal closed
        if closed:
            return
        closed = True
        try:
            if child is not None:
                if child.returncode is None:
                    try:
                        child.kill()
                    except ProcessLookupError:
                        pass
                # Drain the small bounded pipe after killing, to avoid a pipe
                # transport waiting forever when the listener disconnected.
                await child.communicate()
        finally:
            slots.release()
    try:
        child = await asyncio.create_subprocess_exec(
            'ffmpeg', '-nostdin', '-hide_banner', '-loglevel', 'error',
            '-rw_timeout', '30000000', '-protocol_whitelist', 'https,http,tcp,tls,crypto',
            '-i', https_url(url), '-map', '0:a:0', '-vn', '-map_metadata', '-1',
            '-af', 'asetpts=N/SR/TB', '-c:a', 'libmp3lame', '-b:a', '128k',
            '-ar', '44100', '-ac', '2', '-write_xing', '0', '-id3v2_version', '0',
            '-flush_packets', '1', '-f', 'mp3', 'pipe:1',
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            limit=65536)
        # Do not return a misleading HTTP 200 when FFmpeg cannot open the source.
        first = await asyncio.wait_for(child.stdout.read(32768), timeout=45)
        if not first:
            raise RuntimeError('Geen audio ontvangen.')
        async def body():
            try:
                yield first
                while True:
                    chunk = await asyncio.wait_for(child.stdout.read(32768), timeout=45)
                    if not chunk:
                        break
                    yield chunk
                await child.wait()
                if child.returncode:
                    raise RuntimeError('Audiobron onderbroken.')
            finally:
                await close()
        result = Response(mimetype='audio/mpeg', headers=headers)
        result.timeout = None
        from quart.wrappers.response import ResponseBody
        class ManagedBody(ResponseBody):
            def __init__(self):
                self.iterator = body()
            async def __aenter__(self):
                return self.iterator
            async def __aexit__(self, exc_type, exc, tb):
                try:
                    await self.iterator.aclose()
                finally:
                    await close()
        result.response = ManagedBody()
        return result
    except BaseException as exc:
        await close()
        if isinstance(exc, asyncio.CancelledError):
            raise
        if not isinstance(exc, Exception):
            raise
        return Response('Audiobron tijdelijk niet beschikbaar. Probeer opnieuw.', status=502,
                        headers={'Retry-After':'60'})
