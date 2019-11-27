#!/usr/bin/env python3

import argparse
import multiprocessing
import os
import re
import shutil
import sys
import threading
import subprocess
import pathlib
from fnmatch import fnmatch
from abc import ABC, abstractmethod
from urllib.parse import unquote
import concurrent.futures
import shlex

def escape_argument_win(arg):
    # Escape the argument for the cmd.exe shell.
    # See http://blogs.msdn.com/b/twistylittlepassagesallalike/archive/2011/04/23/everyone-quotes-arguments-the-wrong-way.aspx
    #
    # First we escape the quote chars to produce a argument suitable for
    # CommandLineToArgvW. We don't need to do this for simple arguments.

    if not arg or re.search(r'(["\s])', arg):
        arg = '"' + arg.replace('"', r'\"') + '"'

    return escape_for_cmd_exe(arg)

def escape_for_cmd_exe(arg):
    # Escape an argument string to be suitable to be passed to
    # cmd.exe on Windows
    #
    # This method takes an argument that is expected to already be properly
    # escaped for the receiving program to be properly parsed. This argument
    # will be further escaped to pass the interpolation performed by cmd.exe
    # unchanged.
    #
    # Any meta-characters will be escaped, removing the ability to e.g. use
    # redirects or variables.
    #
    # @param arg [String] a single command line argument to escape for cmd.exe
    # @return [String] an escaped string suitable to be passed as a program
    #   argument to cmd.exe

    meta_chars = '()%!^"<>&|'
    meta_re = re.compile('(' + '|'.join(re.escape(char) for char in list(meta_chars)) + ')')
    meta_map = { char: "^%s" % char for char in meta_chars }

    def escape_meta_chars(m):
        char = m.group(1)
        return meta_map[char]

    return meta_re.sub(escape_meta_chars, arg)

VERSION = "3.8"

# DEFAULT CONFIGURATION

# Output folder unless specified
# output = os.path.join(os.environ['HOME'], "Desktop/")
output = os.getcwd()

# Separate torrent output folder (defaults to output):
torrent_dir = output

# Do you want to copy additional files (.jpg, .log, etc)?
copyother = 1

# Specify tracker announce URL
tracker = None

# Max number of threads (e.g., Dual-core = 2, Hyperthreaded Dual-core = 4)
max_threads = multiprocessing.cpu_count()

copy_tags = ('TITLE', 'ALBUM', 'ARTIST', 'GENRE', 'COMMENT', 'DATE', 'TRACK')

# Escape function
escape_str_arg = escape_argument_win if os.name == "nt" else shlex.quote

# Default encoding options
enc_opts = {
    '320':  {'ext': '.mp3',  'opts': ['-b:a', '320k'] },
    'V0':   {'ext': '.mp3',  'opts': ['-q:a', '0'] },
    'V2':   {'ext': '.mp3',  'opts': ['-q:a', '2'] },
    'V8':   {'ext': '.mp3',  'opts': ['-q:a', '8'] },
    'Q8':   {'ext': '.ogg',  'opts': ['-c:a', 'libvorbis', '-qscale:a', '8'] },
    'AAC':  {'ext': '.m4a',  'opts': ['-c:a', 'aac', '-b:a', '320k', '-movflags', '+faststart'] },
    'ALAC': {'ext': '.m4a',  'opts': ['-c:a', 'alac'] },
    'FLAC': {'ext': '.flac', 'opts': ['-c:a', 'flac', '-compression_level', '8', '-sample_fmt', 's16', '-ar', '44100'] },
    'WAV':  {'ext': '.wav',  'opts': [] },
    'AIFF': {'ext': '.aiff', 'opts': ['-map_metadata', '0', '-write_id3v2', '1'] }
}
# END CONFIGURATION

codecs = []

placeholders = {
    'n': 'TRACK',
    't': 'TITLE',
    'a': 'ARTIST',
    'f': '',
    'd': '',
}


def is_audio_file(filename):
    return os.path.splitext(filename)[1].lower() in [".mp3", ".flac", ".m4a", ".wav", ".aiff", ".ogg"]


class Task(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def execute(self, opts, lock):
        pass


class TranscodeTask(Task):
    def __init__(self, source, destination, codec, rename_pattern):
        self.source = source
        # output folder
        self.destination = destination
        self.codec = codec
        self.rename_pattern = rename_pattern
        self.cmd = ["ffmpeg", "-hide_banner", "-v", "warning", "-stats", "-i"]

    def execute(self, opts, lock):
        dest_filename = do_rename(self.rename_pattern, *os.path.split(self.source))
        dest_fullpath = os.path.join(self.destination, dest_filename)

        # replace or add format name in directory
        dest_fullpath = change_format_name(dest_fullpath, self.codec)

        dest_fullpath = os.path.splitext(dest_fullpath)[0] + enc_opts[self.codec]['ext']

        with lock:
            os.makedirs(os.path.dirname(dest_fullpath), exist_ok=True)

        if os.path.exists(dest_fullpath) and not opts.overwrite:
            print("WARN: file %s already exists" % dest_fullpath, file=sys.stderr)
            return 1

        self.cmd.append(self.source)
        self.cmd += enc_opts[self.codec]['opts']
        self.cmd.append(dest_fullpath)

        if opts.verbose:
            print("Encoding", os.path.basename(self.source), "to", dest_fullpath)


        with subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True) as proc:
            print(proc.stderr.read())

        return 0


class CopyTask(Task):
    def __init__(self, source, destination, codec, rename_pattern):
        self.source = source
        self.destination = destination
        self.codec = codec
        self.rename_pattern = rename_pattern

    def execute(self, opts, lock):
        dest_fullpath = ""
        if is_audio_file(self.source):
            dest_filename = do_rename(self.rename_pattern, *os.path.split(self.source))
            dest_fullpath = os.path.join(self.destination, dest_filename)
            dest_fullpath = os.path.splitext(dest_fullpath)[0] + os.path.splitext(self.source)[1]
        else:
            dest_folder, dest_filename = os.path.split(self.source)
            dest_fullpath = os.path.join(self.destination, os.path.basename(dest_folder), dest_filename)
        # replace or add format name in directory
        dest_fullpath = change_format_name(dest_fullpath, self.codec)

        with lock:
            os.makedirs(os.path.dirname(dest_fullpath), exist_ok=True)

        if opts.verbose:
            print("Copying", self.source, "to", dest_fullpath)

        shutil.copy(self.source, dest_fullpath)
        return 0


def filename_from_tags(pattern, tags, dirname, filename):
    if tags is None:
        print("error: renaming, no tags")
        return None

    new_filename = ""
    index = 0
    for match in re.finditer(r"(%\w+%)", pattern):
        pl_is_tag = True
        placeholder = match.group(0)[1:-1]
        if placeholder not in placeholders:
            print("error: unknown placeholder " + placeholder)
            return None
        if len(placeholders[placeholder]) != 0 and placeholders[placeholder] not in tags:
            print("error: " + filename + " no " + placeholders[placeholder] + " tag")
            print(tags)
            return None
        elif len(placeholders[placeholder]) == 0:
            pl_is_tag = False

        new_filename += pattern[index:match.start()]
        if pl_is_tag:
            new_filename += "%(" + placeholders[placeholder] + ")s"
        elif placeholder == 'f':
            new_filename += filename
        elif placeholder == 'd':
            new_filename += os.path.split(dirname)[1]

        index = match.end()
    if index < len(pattern):
        new_filename += escape_percent(pattern[index:])

    if os.path.splitext(filename)[1] != os.path.splitext(new_filename)[1]:
        # add extension again because we are building a new filename in a new variable
        new_filename += os.path.splitext(filename)[1]
    return new_filename % tags


def do_rename(rename_pattern, dirname, filename):
    if not rename_pattern:
        rename_pattern = os.path.join("%d%", "%f%")

    tags = tags_from_file(os.path.join(dirname, filename))

    if placeholders['n'] not in tags:
        failure(1, "{} is missing the TRACK tag".format(filename))

    # the new filename is only the filename (not including the leading directory)
    # filename can contain directories, we need to create the non existing ones
    return filename_from_tags(rename_pattern, tags, dirname, filename)


def tags_from_file(filepath):
    tags = {}

    # get tags using ffmpeg. Maybe there is a better python lib for this?
    # this is consistent with any file format though
    # result is one tag per line, like that:
    # TAG=val
    tagcmd = ["ffmpeg", "-i", filepath, "-f", "ffmetadata", "-"]
    proc = subprocess.Popen(tagcmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    proc_out, _ = proc.communicate()
    for line in proc_out.decode("UTF-8").split("\n"):
        tag = line.split("=")
        if len(tag) != 2 or tag[0].upper() not in copy_tags:
            continue
        # create a dict of tags
        tags[tag[0].upper()] = tag[1].replace(':', '-').replace('/', '-')

    return tags


class EncoderArg(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        super(EncoderArg, self).__init__(option_strings, dest, nargs, **kwargs)
    def __call__(self, parser, namespace, values, option_string=None):
        codecs.append(option_string[2:])


def escape_percent(pattern):
    pattern = re.sub('%', '%%', pattern)
    return pattern


def failure(r, msg):
    print("ERROR: %s: %s" % (r, msg), file=sys.stderr)


def make_torrent(opts, target):
    if opts.verbose:
        print('MAKE: %s.torrent' % target)
    torrent_cmd = "mktorrent -p -a '%s' -o %s.torrent %s 2>&1" % (
        opts.tracker, escape_str_arg(os.path.join(opts.torrent_dir,
                                   os.path.basename(target))),
        escape_str_arg(target)
    )
    if opts.additional:
        torrent_cmd += ' ' + opts.additional
    if opts.nodate:
        torrent_cmd += ' -d'
    if not opts.verbose:
        torrent_cmd += os.devnull
    if opts.verbose:
        print(torrent_cmd)
    r = system(torrent_cmd)
    if r: failure(r, torrent_cmd)


def setup_parser():
    p = argparse.ArgumentParser(
        description="whatmp3 transcodes audio files and creates torrents for them",
        argument_default=False,
        epilog="""depends on ffmpeg and mktorrent""")
    p.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    for a in [
        [['-v', '--verbose'],    False,     'increase verbosity'],
        [['-n', '--notorrent'],  False,     'do not create a torrent after conversion'],
        [['-c', '--original'],   False,     'create a torrent for the original FLAC'],
        [['-i', '--ignore'],     False,     'ignore top level directories without flacs'],
        [['-s', '--silent'],     False,     'do not write to stdout'],
        [['-S', '--skipgenre'],  False,     'do not insert a genre tag in MP3 files'],
        [['-D', '--nodate'],     False,     'do not write the creation date to the .torrent file'],
        [['-L', '--nolog'],      False,     'do not copy log files after conversion'],
        [['-C', '--nocue'],      False,     'do not copy cue files after conversion'],
        [['-H', '--nodots'],     False,     'do not copy dot/hidden files after conversion'],
        [['-w', '--overwrite'],  False,     'overwrite files in output dir'],
        [['-m', '--copyother'],  copyother, 'copy additional files (def: true)'],
    ]:
        p.add_argument(*a[0], **{'default': a[1], 'action': 'store_true', 'help': a[2]})
    for a in [
        [['-a', '--additional'],  None,        'ARGS', 'additional arguments to mktorrent'],
        [['-t', '--tracker'],     tracker,     'URL',  'tracker URL'],
        [['-o', '--output'],      output,      'DIR',  'set output dir'],
        [['-O', '--torrent-dir'], torrent_dir, 'DIR',  'set independent torrent output dir'],
        [['-e', '--rename'],      False,       'PATTERN', 'rename files according to tags according to PATTERN'],
        [['-d', '--root-dir'],    None,        'DIR',     'Replace root directory in Rekordbox collection file'],
    ]:
        p.add_argument(*a[0], **{
            'default': a[1], 'action': 'store',
            'metavar': a[2], 'help': a[3]
        })
    p.add_argument('-T', '--threads', default=max_threads, action='store',
                   dest='max_threads', type=int, metavar='THREADS',
                   help='set number of threads THREADS (def: %s)' % max_threads)
    for enc_opt in enc_opts.keys():
        p.add_argument("--" + enc_opt, action=EncoderArg, nargs=0,
                       help='convert to %s' % (enc_opt))
    p.add_argument('sources', nargs='+', metavar='source',
                   help='directories, playlists or RekordBox collection to transcode')
    return p


def system(cmd):
    return os.system(cmd)


def change_format_name(file_fullpath, codec):
    directory_fullpath, filename = os.path.split(file_fullpath)
    leading_dirs, last_dir = os.path.split(directory_fullpath)

    flacre = re.compile("FLAC|AIFF", re.IGNORECASE)
    if flacre.search(last_dir):
        return os.path.join(leading_dirs, flacre.sub(codec, last_dir), filename)
    else:
        return os.path.join(leading_dirs, last_dir + " (" + codec + ")", filename)


def task_dispatch(filename_fullpath, thread_ex, codec, opts, lock):
    if fnmatch(filename_fullpath.lower(), "*.flac") or fnmatch(filename_fullpath.lower(), "*.aiff"):
        transcode_task = TranscodeTask(filename_fullpath, opts.output, codec, opts.rename)
        thread_ex.submit(transcode_task.execute, opts, lock)
    else:
        copy_task = CopyTask(filename_fullpath, opts.output, codec, opts.rename)
        thread_ex.submit(copy_task.execute, opts, lock)


def parse_folder(folder, thread_ex, codec, opts, lock):
    folder = os.path.abspath(folder)
    if not os.path.exists(opts.torrent_dir):
        os.makedirs(opts.torrent_dir)
    for dirpath, _, files in os.walk(folder, topdown=False):
        for filename in files:
            task_dispatch(os.path.join(dirpath, filename), thread_ex, codec, opts, lock)

def parse_m3u(playlist_filename, thread_ex, codec, opts, lock):
    with open(playlist_filename) as playlist_file:
        for _, line in enumerate(playlist_file):
            if line[0] == '#':
                continue
            track_file = line.rstrip("\r\n")
            if not is_audio_file(track_file):
                continue
            if not os.path.exists(track_file) or not os.path.isfile(track_file):
                failure(track_file, "does not exist")
                continue

            opts.rename = opts.rename if opts.rename else "%f%"

            task_dispatch(track_file, thread_ex, codec, opts, lock)



def parse_xml_playlists(node, collection_root, thread_ex, codec, opts, lock):
    if node.attrib["Type"] == "0":
        for child in node:
            parse_xml_playlists(child, collection_root, thread_ex, codec, opts, lock)
    else:

        for child in node:
            track_id = child.attrib['Key']
            track_node = collection_root.find(f"./TRACK[@TrackID='{track_id}']")

            track_path = pathlib.Path(track_node.attrib['Location'])
            track_path = pathlib.Path(opts.root_dir).joinpath(pathlib.Path(*track_path.parts[3:])) if opts.root_dir else pathlib.Path(*track_path[2:])

            playlist_name = node.attrib['Name']
            opts.rename = f"{playlist_name}/%f%"

            task_dispatch(unquote(str(track_path)), thread_ex, codec, opts, lock)

def parse_xml(xml_filename, thread_ex, codec, opts, lock):
    import xml.etree.ElementTree as ET
    tree = ET.parse(xml_filename)
    root = tree.getroot()

    playlists_root = root.find("PLAYLISTS")
    collection_root = root.find("COLLECTION")

    parse_xml_playlists(playlists_root[0], collection_root, thread_ex, codec, opts, lock)


def main():
    parser = setup_parser()
    opts = parser.parse_args()

    if len(codecs) == 0 and not opts.original and not opts.rename:
        parser.error("you must provide at least one format to transcode to")
        exit()

    lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=opts.max_threads) as thread_ex:
        for codec in codecs:
            for source_dir in opts.sources:
                extension = os.path.splitext(source_dir)[1]
                if extension in ("m3u", "m3u8"):
                    parse_m3u(source_dir, thread_ex, codec, opts, lock)
                elif extension == ".xml":
                    parse_xml(source_dir, thread_ex, codec, opts, lock)
                else:
                    parse_folder(source_dir, thread_ex, codec, opts, lock)


if __name__ == '__main__':
    main()
