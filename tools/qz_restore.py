#!/usr/bin/env python

"""Generic upload script for qizx"""
from argparse import ArgumentParser
import logging
from multiprocessing import Process, Queue
from qizx import bulk
import sys

def _uploader(url, archiver, queue):
    try:
        r = bulk.Restore(url, archiver, queue)
        r.restore_queue(queue)
    except KeyboardInterrupt:
        # Just swallow keyboard interrupts and stop
        pass

class Shell:
    def main(self, argv):
        # parse command line
        parser = ArgumentParser(description = 'Restore a qizx database.')
        parser.add_argument(
            '--verbose',
            action = 'store_true',
            help = 'Turn on verbose logging.'
        )
        parser.add_argument(
            '--debug',
            action = 'store_true',
            help = 'Turn on debug logging.'
        )
        parser.add_argument(
            'url',
            help = 'Database URL. This can be a full URL (e.g. '
            'http://qizx-admin:changeit@myserver.mycompany.com:8080/qizx/api) '
            'or a section in the .qizx config file.'
        )
        parser.add_argument(
            '-d',
            '--directory',
            default='.',
            help='Directory to read.'
        )
        parser.add_argument(
            '-l',
            '--library',
            help="Database library. If not set, top level directories are used "
            "for library names."
        )
        parser.add_argument(
            '--jobs',
            '-J',
            type=int,
            default=8,
            help='Number of jobs to run concurrently.'
        )
        parser.add_argument(
            '--tar',
            '-t',
            help='Read an uncompressed tar file.'
        )
        parser.add_argument(
            '--gzip',
            '-z',
            action = 'store_true',
            help='Read a gzip compressed tar file.'
        )
        parser.add_argument(
            '--bzip2',
            '-j',
            action = 'store_true',
            help='Read a bzip2 compressed tar file.'
        )

        config = parser.parse_args(argv[1:])

        # configure logging
        if config.debug:
            lvl = logging.DEBUG
        elif config.verbose:
            lvl = logging.INFO
        else:
            lvl = logging.WARNING
        logging.basicConfig(level=lvl, format="%(asctime)s: %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S", stream=sys.stdout)

        if config.tar is not None:
            mode = "r:gz" if config.gzip else "r:bz2" if config.bzip2 else "r"
            a = lambda: bulk.Tar(config.tar, mode)
        else:
            a = lambda: bulk.Directory(config.directory)

        exitcode = 0

        if config.jobs == 1:
            r = bulk.Restore(config.url, a)

        else:
            q = Queue()

            procs = []
            for i in range(config.jobs - 1):
                proc = Process(target=_uploader, args=(config.url, a, q))
                proc.start()
                procs.append(proc)

            d = bulk.Restore(config.url, a, q)

        r.restore_lib(config.library)

        if config.jobs > 1:
            try:
                for i in range(len(procs)):
                    q.put(None)

                for pr in procs:
                    pr.join()
                    logging.debug('proc %s returned %d' % (pr.name, pr.exitcode))

                    if not exitcode and pr.exitcode == 1:
                        logging.warn(
                            '%s - At least one document failed during upload.'
                            % pr.name
                        )
                        exitcode = 1
                    elif exitcode < 2 and pr.exitcode == 2:
                        logging.warn('Fatal error received during upload.'
                                                                    % pr.name)
                        exitcode = 2
                    elif exitcode < 100 and pr.exitcode == 100:
                        logging.warn(
                                "Couldn't create client, check config & url.")
                        exitcode = 100
            except KeyboardInterrupt:
                logging.warn(
                        'Aborting on CTRL-C - waiting for workers to finish.')
                exitcode = -1
                for pr in procs:
                    pr.join()

            q.close()

        r.archiver.close()

        return exitcode


if __name__ == "__main__":
    sys.exit(Shell().main(sys.argv))
