#!/usr/bin/env python3

import argparse
import multiprocessing
import os
import re
import shutil
import sys
import threading
from fnmatch import fnmatch
import multiprocessing
import shlex

VERSION = "3.8"

# DEFAULT CONFIGURATION

# Output folder unless specified
# output = os.path.join(os.environ['HOME'], "Desktop/")
output = os.getcwd()

# Separate torrent output folder (defaults to output):
torrent_dir = output

# Do you want to copy additional files (.jpg, .log, etc)?
copyother = 1

# Do you want to zeropad tracknumbers? (1 => 01, 2 => 02 ...)
zeropad = 1

# Specify tracker announce URL
tracker = None

# Max number of threads (e.g., Dual-core = 2, Hyperthreaded Dual-core = 4)
max_threads = multiprocessing.cpu_count()

# Default encoding options
enc_opts = {
    '320':  {'ext': '.mp3',  'opts': '-b:a 320k'},
    'V0':   {'ext': '.mp3',  'opts': '-q:a 0'},
    'V2':   {'ext': '.mp3',  'opts': '-q:a 2'},
    'V8':   {'ext': '.mp3',  'opts': '-q:a 8'},
    'Q8':   {'ext': '.ogg',  'opts': '-c:a libvorbis -qscale:a 8'},
    'AAC':  {'ext': '.m4a',  'opts': '-c:a aac -b:a 320k -movflags +faststart'},
    'ALAC': {'ext': '.m4a',  'opts': '-c:a alac'},
    'FLAC': {'ext': '.flac', 'opts': '-c:a flac -sample_fmt s16 -ar 44100'}
}

ffmpeg_cmd = "ffmpeg -hide_banner -v warning -stats -i %(infile)s %(opts)s %(filename)s 2>&1"

# END CONFIGURATION

codecs = []

def copy_other(opts, flacdir, outdir):
    if opts.verbose:
        print('COPYING other files')
    for dirpath, dirs, files in os.walk(flacdir, topdown=False):
        for name in files:
            if opts.nolog and fnmatch(name.lower(), '*.log'):
                continue
            if opts.nocue and fnmatch(name.lower(), '*.cue'):
                continue
            if opts.nodots and fnmatch(name.lower(), '^.'):
                continue
            if (not fnmatch(name.lower(), '*.flac')
               and not fnmatch(name.lower(), '*.m3u')):
                d = re.sub(re.escape(flacdir), outdir, dirpath)
                if (os.path.exists(os.path.join(d, name))
                   and not opts.overwrite):
                    continue
                if not os.path.exists(d):
                    os.makedirs(d)
                shutil.copy(os.path.join(dirpath, name), d)

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
        opts.tracker, shlex.quote(os.path.join(opts.torrent_dir,
                                   os.path.basename(target))),
        shlex.quote(target)
    )
    if opts.additional:
        torrent_cmd += ' ' + opts.additional
    if opts.nodate:
        torrent_cmd += ' -d'
    if not opts.verbose:
        torrent_cmd += ' >/dev/null'
    if opts.verbose:
        print(torrent_cmd)
    r = system(torrent_cmd)
    if r: failure(r, torrent_cmd)

def replaygain(opts, codec, outdir):
    if opts.verbose:
        print("APPLYING replaygain")
        print(encoders[enc_opts[codec]['enc']]['regain'] % outdir)
    r = system(encoders[enc_opts[codec]['enc']]['regain'] % shlex.quote(outdir))
    if r: failure(r, "replaygain")
    for dirpath, dirs, files in os.walk(outdir, topdown=False):
        for name in dirs:
            r = system(encoders[enc_opts[codec]['enc']]['regain']
                       % shlex.quote(os.path.join(dirpath, name)))
            if r: failure(r, "replaygain")

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
        [['-r', '--replaygain'], False,     'add ReplayGain to new files'],
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
        [['-z', '--zeropad'],    zeropad,   'zeropad tracknumbers (def: true)'],
    ]:
        p.add_argument(*a[0], **{'default': a[1], 'action': 'store_true', 'help': a[2]})
    for a in [
        [['-a', '--additional'],  None,        'ARGS', 'additional arguments to mktorrent'],
        [['-t', '--tracker'],     tracker,     'URL',  'tracker URL'],
        [['-o', '--output'],      output,      'DIR',  'set output dir'],
        [['-O', '--torrent-dir'], torrent_dir, 'DIR',  'set independent torrent output dir'],
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
    p.add_argument('flacdirs', nargs='+', metavar='flacdir',
                   help='directories to transcode')
    return p

def system(cmd):
    return os.system(cmd)

def transcode(f, flacdir, mp3_dir, codec, opts, lock):
    outname = re.sub(re.escape(flacdir), mp3_dir, f)
    outname = re.sub(re.compile('\.flac$', re.IGNORECASE), '', outname)
    with lock:
        if not os.path.exists(os.path.dirname(outname)):
            os.makedirs(os.path.dirname(outname))
    outname += enc_opts[codec]['ext']
    if os.path.exists(outname) and not opts.overwrite:
        print("WARN: file %s already exists" % (os.path.relpath(outname)),
              file=sys.stderr)
        return 1
    flac_cmd = ffmpeg_cmd % {
        'opts': enc_opts[codec]['opts'],
        'infile': escape_percent(shlex.quote(f)),
        'filename': shlex.quote(outname),
    }
    outname = os.path.basename(outname)
    if not opts.silent:
        print("encoding %s" % outname)
    if opts.verbose:
        print(flac_cmd)
    r = system(flac_cmd)
    if r:
        failure(r, "error encoding %s" % outname)
        system("touch '%s/FAILURE'" % mp3_dir)
    return 0

class Transcode(threading.Thread):
    def __init__(self, file, flacdir, mp3_dir, codec, opts, lock, cv):
        threading.Thread.__init__(self)
        self.file = file
        self.flacdir = flacdir
        self.mp3_dir = mp3_dir
        self.codec = codec
        self.opts = opts
        self.lock = lock
        self.cv = cv

    def run(self):
        r = transcode(self.file, self.flacdir, self.mp3_dir, self.codec,
                      self.opts, self.lock)
        with self.cv:
            self.cv.notify_all()
        return r

def main():
    parser = setup_parser()
    opts = parser.parse_args()
    if not opts.output.endswith('/'):
        opts.output += '/'
    if len(codecs) == 0 and not opts.original:
        parser.error("you must provide at least one format to transcode to")
        exit()
    for flacdir in opts.flacdirs:
        flacdir = os.path.abspath(flacdir)
        flacfiles = []
        if not os.path.exists(opts.torrent_dir):
            os.makedirs(opts.torrent_dir)
        for dirpath, dirs, files in os.walk(flacdir, topdown=False):
            for name in files:
                if fnmatch(name.lower(), '*.flac'):
                    flacfiles.append(os.path.join(dirpath, name))
        flacfiles.sort()
        if opts.ignore and not flacfiles:
            if not opts.silent:
                print("SKIP (no flacs in): %s" % (os.path.relpath(flacdir)))
            continue
        if opts.original:
            if not opts.silent:
                print('BEGIN ORIGINAL FLAC')
            if opts.output and opts.tracker and not opts.notorrent:
                make_torrent(opts, flacdir)
            if not opts.silent:
                print('END ORIGINAL FLAC')

        for codec in codecs:
            outdir = os.path.basename(flacdir)
            flacre = re.compile('FLAC', re.IGNORECASE)
            if flacre.search(outdir):
                outdir = flacre.sub(codec, outdir)
            else:
                outdir = outdir + " (" + codec + ")"
            outdir = opts.output + outdir
            if not os.path.exists(outdir):
                os.makedirs(outdir)

            if not opts.silent:
                print('BEGIN ' + codec + ': %s' % os.path.relpath(flacdir))
            threads = []
            cv = threading.Condition()
            lock = threading.Lock()
            for f in flacfiles:
                with cv:
                    while (threading.active_count() == max(1, opts.max_threads) + 1):
                        cv.wait()
                    t = Transcode(f, flacdir, outdir, codec, opts, lock, cv)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()

            if opts.copyother:
                copy_other(opts, flacdir, outdir)
            if opts.replaygain:
                replaygain(opts, codec, outdir)
            if opts.output and opts.tracker and not opts.notorrent:
                make_torrent(opts, outdir)
            if not opts.silent:
                print('END ' + codec + ': %s' % os.path.relpath(flacdir))

        if opts.verbose: print('ALL DONE: ' + os.path.relpath(flacdir))
    return 0

if __name__ == '__main__':
    main()
