"""Manually export one short episode and an isolated RSS compatibility test."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import secrets
import tempfile
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET

ITUNES = "http://www.itunes.com/dtds/podcast-1.0.dtd"
ET.register_namespace("itunes", ITUNES)


def select_episode(episodes):
    available = [ep for ep in episodes if any(
        (ep.get(key) or {}).get("url") for key in ("audio", "streamMedia")
    )]
    if not available:
        raise RuntimeError("Geen aflevering met een audiolink gevonden.")
    trailers = [ep for ep in available if "trailer" in ep.get("title", "").lower()]

    def duration(ep):
        values = [(ep.get(key) or {}).get("duration") for key in ("audio", "streamMedia")]
        return next((float(v) for v in values if v and float(v) > 0), float("inf"))

    return min(trailers or available, key=duration)


def rss_bytes(podcast, episode, base, token, size, duration):
    root = ET.Element("rss", version="2.0")
    channel = ET.SubElement(root, "channel")
    def node(parent, tag, text):
        ET.SubElement(parent, tag).text = str(text)
    title = (podcast.get("title") or episode.get("podcastName") or "Podimo") + " - M4A test"
    node(channel, "title", title)
    node(channel, "link", base)
    node(channel, "description", "Prive compatibiliteitstest met een enkele aflevering.")
    node(channel, "language", podcast.get("language") or "nl-NL")
    node(channel, f"{{{ITUNES}}}block", "yes")
    node(channel, f"{{{ITUNES}}}author", podcast.get("authorName") or "Podimo")
    item = ET.SubElement(channel, "item")
    node(item, "title", episode.get("title") or "Testaflevering")
    node(item, "description", episode.get("description") or "Testaflevering")
    guid = ET.SubElement(item, "guid", isPermaLink="false")
    guid.text = "podimo-m4a-test-" + episode["id"]
    raw_date = episode.get("publishDatetime") or episode.get("datetime")
    if raw_date:
        try:
            date = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
        except ValueError:
            date = parsedate_to_datetime(str(raw_date))
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
    else:
        date = datetime.now(timezone.utc)
    node(item, "pubDate", format_datetime(date))
    node(item, f"{{{ITUNES}}}duration", round(duration))
    ET.SubElement(item, "enclosure", {
        "url": f"{base}/pocketcasts-test/{token}/episode.m4a",
        "length": str(size),
        "type": "audio/mp4",
    })
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


async def run_process(*args):
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=600)
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.communicate()
        raise
    if process.returncode:
        detail = re.sub(r"https?://\S+", "[URL verborgen]", stderr.decode(errors="replace"))
        raise RuntimeError(f"{args[0]} mislukt:\n{detail[-2000:]}")
    return stdout


async def export(podcast_id):
    # Imported only here so helper tests need no service credentials.
    import main as service
    import cloudscraper

    if not service.LOCAL_CREDENTIALS or not service.PODIMO_EMAIL or not service.PODIMO_PASSWORD:
        raise RuntimeError("LOCAL_CREDENTIALS, PODIMO_EMAIL en PODIMO_PASSWORD moeten ingesteld zijn.")
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", podcast_id):
        raise RuntimeError("Ongeldig podcast-ID.")
    base = f"{service.PODIMO_PROTOCOL}://{service.PODIMO_HOSTNAME}".rstrip("/")
    print("Afleveringsgegevens ophalen...", flush=True)
    with cloudscraper.create_scraper() as scraper:
        if service.HTTP_PROXY:
            scraper.proxies = {"https": service.HTTP_PROXY}
        client = await service.check_auth(
            service.PODIMO_EMAIL, service.PODIMO_PASSWORD, "nl", "nl-NL", scraper
        )
        if client is None:
            raise RuntimeError("Inloggen op Podimo mislukt.")
        data = await client.getPodcasts(podcast_id, scraper)

    episode = select_episode(data["episodes"])
    url, _ = service.extract_audio_url(episode)
    if not url or urlsplit(url).scheme != "https":
        raise RuntimeError("Geen HTTPS-audiolink beschikbaar.")
    print(f"Een aflevering verwerken: {episode.get('title', episode['id'])}", flush=True)
    root = Path(service.CACHE_DIR) / "pocketcasts-test"
    root.mkdir(parents=True, exist_ok=True)
    token = secrets.token_hex(32)
    destination = root / token
    with tempfile.TemporaryDirectory(prefix="building-", dir=root) as temporary:
        directory = Path(temporary)
        audio = directory / "episode.m4a"
        await run_process(
            "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
            "-rw_timeout", "30000000",
            "-protocol_whitelist", "https,http,tcp,tls,crypto",
            "-i", url,
            "-map", "0:a:0",
            "-vn",
            "-af", "asetpts=N/SR/TB",
            "-c:a", "aac",
            "-b:a", "96k",
            "-movflags", "+faststart",
            "-y", str(audio),
        )
        probe = json.loads(await run_process(
            "ffprobe", "-v", "error", "-select_streams", "a:0",
            "-show_entries", "stream=codec_name:format=duration", "-of", "json", str(audio),
        ))
        duration = float(probe.get("format", {}).get("duration", 0))
        if not probe.get("streams") or duration <= 0 or audio.stat().st_size == 0:
            raise RuntimeError("Het omgezette bestand bevat geen geldige audio.")
        feed = rss_bytes(data["podcast"], episode, base, token, audio.stat().st_size, duration)
        (directory / "feed.xml").write_bytes(feed)
        # Publish only after both files are complete; preserve all other cache entries.
        os.rename(directory, destination)
    print("Klaar. Voeg deze URL als PRIVATE feed toe aan Pocket Casts:", flush=True)
    print(f"{base}/pocketcasts-test/{token}/feed.xml", flush=True)
    print("Deze geheime URL geeft toegang tot de testaflevering; deel hem niet openbaar.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("podcast_id")
    args = parser.parse_args()
    try:
        asyncio.run(export(args.podcast_id))
    except (RuntimeError, asyncio.TimeoutError) as error:
        parser.exit(1, f"Test mislukt: {error}\n")
