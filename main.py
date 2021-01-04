import logging
from pymediainfo import MediaInfo
import os
import sqlite3
import subprocess

TEMP_DIR = 'H:\\TranscodeTemp'
FFMPEG_PATH = 'ffmpeg.exe'
MEDIA_DIR = 'H:\\Videos'

class MediaInfoCache:
    def __init__(self, db='media.db'):
        # Create sqlite connection
        self._conn = sqlite3.connect(db)

        # Create schema if necessary
        c = self._conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS `files` ( `path` TEXT NOT NULL, `size` INTEGER NOT NULL, `modified` REAL NOT NULL, `created` REAL NOT NULL, `mediainfo` TEXT NOT NULL, PRIMARY KEY(`path`) )')
        self._conn.commit()

    def get_media_info(self, path: str) -> MediaInfo:
        # Get file attributes
        ctime = os.path.getctime(path)
        mtime = os.path.getmtime(path)
        size = os.path.getsize(path)

        # Try read from cache first
        c = self._conn.cursor()
        c.execute('SELECT mediainfo FROM files WHERE path=? AND size=? AND modified=? AND created=?',
                  (path, size, mtime, ctime))
        row = c.fetchone()

        if row is None:
            logging.debug('Parsing MediaInfo from %s', path)

            # Parse file to get media info
            media_info_xml = MediaInfo.parse(path, output='OLDXML')
            media_info = MediaInfo(media_info_xml)

            # Update cache
            c.execute('REPLACE INTO files (path, size, modified, created, mediainfo) VALUES (?, ?, ?, ?, ?)',
                      (path, size, mtime, ctime, media_info_xml))
            self._conn.commit()
            return media_info
        else:
            return MediaInfo(row[0])


def should_transcode_audio(media_info: MediaInfo) -> bool:
    # We only care about files which have a video track and a DTS audio track
    has_video = False
    has_dts = False
    for track in media_info.tracks:
        if track.track_type == 'Video':
            has_video = True
        elif track.track_type == 'Audio' and track.format == 'DTS':
            has_dts = True
    return has_video and has_dts


def process_file(cache: MediaInfoCache, path: str):
    # Check whether we need to transcode
    media_info = cache.get_media_info(path)
    if not should_transcode_audio(media_info):
        return

    # Get temp file name
    filename = os.path.basename(path)
    temp_path = os.path.join(TEMP_DIR, filename)

    # Transcode to temp directory
    logging.info('Transcoding %s to %s', path, temp_path)
    res = subprocess.run([FFMPEG_PATH, '-i', path, '-c:v', 'copy', '-c:a',
                          'libfdk_aac', '-vbr', '5', temp_path], capture_output=True)
    if res.returncode != 0:
        logging.error(
            'Failed to transcode file, stdout:\n%s\n\nstderr:\n%s', res.stdout, res.stderr)
        try:
            os.remove(temp_path)
        except:
            pass
        return

    logging.info('Successfully transcoded %s', path)

    # Rename file in temp folder over the top of actual one
    os.replace(temp_path, path)

    # Recompute media info and save
    cache.get_media_info(path)


# Setup logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO, handlers=[
    logging.FileHandler('media-fixer.log'),
    logging.StreamHandler()
])

# Create temp folder
os.makedirs(TEMP_DIR, exist_ok=True)

# Process files
cache = MediaInfoCache()
for root, dirs, files in os.walk(MEDIA_DIR):
    logging.debug('Processing %s', root)
    for file in files:
        path = os.path.join(root, file)
        try:
            process_file(cache, path)
        except:
            logging.exception('Failed to process file')
