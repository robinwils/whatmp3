#!/usr/bin/env python3

import argparse
import multiprocessing
import os
import re
import shutil
import sys
import threading
from fnmatch import fnmatch
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

# NULL device for error redirections
dev_null = "NUL" if os.name == "nt" else "/dev/null"

# Escape function
escape_str_arg = escape_argument_win if os.name == "nt" else shlex.quote

# Default encoding options
enc_opts = {
    '320':  {'ext': '.mp3',  'opts': '-b:a 320k'},
    'V0':   {'ext': '.mp3',  'opts': '-q:a 0'},
    'V2':   {'ext': '.mp3',  'opts': '-q:a 2'},
    'V8':   {'ext': '.mp3',  'opts': '-q:a 8'},
    'Q8':   {'ext': '.ogg',  'opts': '-c:a libvorbis -qscale:a 8'},
    'AAC':  {'ext': '.m4a',  'opts': '-c:a aac -b:a 320k -movflags +faststart'},
    'ALAC': {'ext': '.m4a',  'opts': '-c:a alac'},
    'FLAC': {'ext': '.flac', 'opts': '-c:a flac -compression_level 8 -sample_fmt s16 -ar 44100'},
    'WAV':  {'ext': '.wav',  'opts': ''},
    'AIFF': {'ext': '.aiff', 'opts': '-map_metadata 0 -write_id3v2 1'}
}

ffmpeg_cmd = "ffmpeg -hide_banner -v warning -stats -i %(infile)s %(opts)s %(filename)s 2>&1"

# END CONFIGURATION

codecs = []

placeholders = {
    'n': 'TRACK',
    't': 'TITLE',
    'a': 'ARTIST',
    'f': '',
    'd': '',
}

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
    return new_filename % tags

def do_rename(rename_pattern, dirname, filename):
    if not rename_pattern:
        rename_pattern = os.path.join("%d%", "%f%")

    tags = tags_from_file(os.path.join(dirname, filename))
    try:
        tags[placeholders['n']] = tags[placeholders['n']].split('/')[0]
    except KeyError as key_error:
        failure(1, "{} is missing the TRACK tag".format(filename))

    # the new filename is only the filename (not including the leading directory)
    # filename can conatin directories, we need to create the non existing ones
    return filename_from_tags(rename_pattern, tags, dirname, filename)



def tags_from_file(filepath):
    tags = {}

    # get tags using ffmpeg. Maybe there is a better python lib for this?
    # this is consistent with any file format though
    # result is one tag per line, like that:
    # TAG=val
    tagcmd = "ffmpeg -i {} -f ffmetadata - 2> {}".format(escape_str_arg(filepath), dev_null)
    for line in os.popen(tagcmd).read().rstrip().splitlines():
        tag = line.split("=")
        if len(tag) != 2 or tag[0].upper() not in copy_tags:
            continue
        # create a dict of tags
        tags[tag[0].upper()] = tag[1]

    return tags


def parse_m3u(opts, playlist_filename, files_to_transcode):
    with open(playlist_filename) as playlist_file:
        for _, line in enumerate(playlist_file):
            if line[0] == '#':
                continue
            track_file = line.rstrip("\r\n")
            if not os.path.exists(track_file) or not os.path.isfile(track_file):
                failure(track_file, "does not exist")
            dirpath, filename = os.path.split(track_file)

            # rename with pattern that is only the filename, to get all files in the same folder
            new_filename = do_rename(opts.rename if opts.rename else "%f%", dirpath, filename)
            files_to_transcode[track_file] = os.path.join(opts.output, new_filename)
    print(files_to_transcode)


def parse_folder(opts, folder, files_to_transcode, files_to_copy):
    folder = os.path.abspath(folder)
    if not os.path.exists(opts.torrent_dir):
        os.makedirs(opts.torrent_dir)
    for dirpath, _, files in os.walk(folder, topdown=False):
        new_dir = ""
        for name in files:
            if (fnmatch(name.lower(), '*.flac')
                or fnmatch(name.lower(), '*.aiff')):
                new_filename = do_rename(opts.rename, dirpath, name)
                if not new_filename:
                    continue
                flacfile = os.path.join(dirpath, name)
                files_to_transcode[flacfile] = os.path.join(opts.output, new_filename)
                new_dir, _ = os.path.split(files_to_transcode[flacfile])
                if not new_dir:
                    new_dir, _ = os.path.split(files_to_transcode[flacfile])
            elif opts.copyother and new_dir:
                if new_dir not in files_to_copy:
                    files_to_copy[new_dir] = []
                files_to_copy[new_dir].append(os.path.join(dirpath, name))

    if opts.ignore and not files_to_transcode:
        if not opts.silent:
            print("SKIP (no flacs in): %s" % (os.path.relpath(folder)))
        return
    if opts.original:
        if not opts.silent:
            print('BEGIN ORIGINAL FLAC')
        if opts.output and opts.tracker and not opts.notorrent:
            make_torrent(opts, folder)
        if not opts.silent:
            print('END ORIGINAL FLAC')

def copy_other(opts, files, outdir):
    if opts.verbose:
        print('COPYING other files')
    for name in files:
        if opts.nolog and fnmatch(name.lower(), '*.log'):
            continue
        if opts.nocue and fnmatch(name.lower(), '*.cue'):
            continue
        if opts.nodots and fnmatch(name.lower(), '^.'):
            continue
        if (not fnmatch(name.lower(), '*.flac')
           and not fnmatch(name.lower(), '*.m3u')):
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            shutil.copy(name, os.path.join(outdir, os.path.split(name)[1]))

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
        print('MAKE: %s.torrent' % os.path.relpath(target))
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
        torrent_cmd += dev_null
    if opts.verbose:
        print(torrent_cmd)
    r = system(torrent_cmd)
    if r: failure(r, torrent_cmd)

def setup_parser():
    p = argparse.ArgumentParser(
        description="whatmp3 transcodes audio files and creates torrents for them",
        argument_default=False,
        epilog="""depends on flac, metaflac, mktorrent, and optionally oggenc, lame, neroAacEnc,
        neroAacTag, mp3gain, aacgain, vorbisgain, and sox""")
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
                   help='directories or playlists to transcode')
    return p

def system(cmd):
    return os.system(cmd)

def transcode(infile, outfile, codec, opts, lock):
    outname = outfile + enc_opts[codec]['ext']
    with lock:
        os.makedirs(os.path.dirname(outname), exist_ok=True)
    if os.path.exists(outname) and not opts.overwrite:
        print("WARN: file %s already exists" % (os.path.relpath(outname)),
              file=sys.stderr)
        return 1
    flac_cmd = ffmpeg_cmd % {
        'opts': enc_opts[codec]['opts'],
        'infile': escape_percent(escape_str_arg(infile)),
        'filename': escape_str_arg(outname),
    }
    outname = os.path.basename(outname)
    if not opts.silent:
        print("encoding %s" % outname)
    if opts.verbose:
        print(flac_cmd)
    r = system(flac_cmd)
    if r:
        failure(r, "error encoding %s" % outname)
        system("touch '%s/FAILURE'" % outfile)
    return 0


def change_format_name(directory, informat, codec):
    directory = directory.rstrip('/')
    last_slash_idx = directory.rfind('/')
    leading_dirs = directory[0:last_slash_idx + 1]
    last_dir = directory[last_slash_idx + 1:]

    flacre = re.compile(informat, re.IGNORECASE)
    if flacre.search(last_dir):
        return leading_dirs + flacre.sub(codec, last_dir)
    else:
        return leading_dirs + last_dir + " (" + codec + ")"

def main():
    parser = setup_parser()
    opts = parser.parse_args()
    if len(codecs) == 0 and not opts.original and not opts.rename:
        parser.error("you must provide at least one format to transcode to")
        exit()
    files_to_transcode = {}
    files_to_copy = {}
    outdir = ""
    for flacdir in opts.sources:
        if os.path.isfile(flacdir):
            parse_m3u(opts, flacdir, files_to_transcode)
        else:
            parse_folder(opts, flacdir, files_to_transcode, files_to_copy)
    for codec in codecs:
        if not opts.silent:
            print('BEGIN ' + codec + ': %s' % os.path.relpath(flacdir))
        lock = threading.Lock()
        with concurrent.futures.ThreadPoolExecutor(max_workers=opts.max_threads) as ex:
            for infile, outfile in files_to_transcode.items():
                (dirs, filename) = os.path.split(outfile)
                _, fext = os.path.splitext(infile)
                out_filename, _ = os.path.splitext(filename)
                outdir = change_format_name(dirs, fext[1:].upper(), codec)
                if opts.copyother and dirs in files_to_copy:
                    copy_other(opts, files_to_copy[dirs], outdir)
                ex.submit(transcode, infile, os.path.join(outdir, out_filename), codec, opts, lock)

        if opts.output and opts.tracker and not opts.notorrent:
            make_torrent(opts, outdir)
        if not opts.silent:
            print('END ' + codec + ': %s' % os.path.relpath(flacdir))

        if opts.verbose: print('ALL DONE: ' + os.path.relpath(flacdir))
    return 0

if __name__ == '__main__':
    main()
