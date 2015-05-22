#!/usr/bin/env python

"""Generic download script for qizx"""
from argparse import ArgumentParser
import logging
from multiprocessing import Process, Queue, Manager
from .qizx import bulk
import sys

def _downloader(url, archiver, queue):
    try:
        d = bulk.Dump(url, archiver, queue)
        d.dump_queue(queue)
    except KeyboardInterrupt:
        # Just swallow keyboard interrupts and stop
        pass

def _tar_writer(archiver, queue):
    try:
        a = archiver()
        while True:
            job = queue.get()
            if job is None:
                return
            (name, value) = job
            a.write(name, value)
        a.close()

    except KeyboardInterrupt:
        # Just swallow keyboard interrupts and stop
        pass

    a.close()

class Shell:
    def main(self, argv):
        # parse command line
        parser = ArgumentParser(description = 'Dump a qizx database.')
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
            help='Directory to write to.'
        )
        parser.add_argument(
            '-l',
            '--library',
            help="Database library. All libraries are dumped if this isn't set."
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
            help='Output to an uncompressed tar file.'
        )
        parser.add_argument(
            '--gzip',
            '-z',
            action = 'store_true',
            help='Output to a gzip compressed tar file.'
        )
        parser.add_argument(
            '--bzip2',
            '-j',
            action = 'store_true',
            help='Output to a bzip2 compressed tar file.'
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
            mode = "w:gz" if config.gzip else "w:bz2" if config.bzip2 else "w"
            a = lambda: bulk.Tar(config.tar, mode)
        else:
            a = lambda: bulk.Directory(config.directory)

        exitcode = 0

        if config.jobs == 1:
            d = bulk.Dump(config.url, a)

        else:
            q = Queue()

            if config.tar is not None:
                tar_queue = Manager().Queue()
                tar_proc = Process(target=_tar_writer, args=(a, tar_queue))
                tar_proc.start()
                a = lambda: bulk.TarQueue(tar_queue)

            procs = []
            for i in range(config.jobs - 1):
                proc = Process(target=_downloader, args=(config.url, a, q))
                proc.start()
                procs.append(proc)

            d = bulk.Dump(config.url, a, q)

        if config.library is None:
            libs = d.db.listlib()
        else:
            libs = [config.library]

        for lib in libs:
            d.dump_lib(lib)

        if config.jobs > 1:
            try:
                #
                # Terminate the job queue
                #
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
                        logging.warn('Fatal error received during upload.' %pr.name)
                        exitcode = 2
                    elif exitcode < 100 and pr.exitcode == 100:
                        logging.warn("Couldn't create client, check config & url.")
                        exitcode = 100

            except KeyboardInterrupt:
                logging.warn('Aborting on CTRL-C - waiting for workers to finish.')
                exitcode = -1
                for pr in procs:
                    pr.join()

            q.close()

            try:
                if config.tar is not None:
                    tar_queue.put(None)

                    tar_proc.join()
                    logging.debug('proc %s returned %d'
                                        % (tar_proc.name, tar_proc.exitcode))

                    if not exitcode and tar_proc.exitcode == 1:
                        logging.warn(
                            '%s - At least one document failed during upload.'
                            % tar_proc.name
                        )
                        exitcode = 1
                    elif exitcode < 2 and tar_proc.exitcode == 2:
                        logging.warn('Fatal error received during upload.'
                                                                % tar_proc.name)
                        exitcode = 2
                    elif exitcode < 100 and tar_proc.exitcode == 100:
                        logging.warn("Couldn't create client, check config "
                                                                    "& url.")
                        exitcode = 100

            except KeyboardInterrupt:
                logging.warn('Aborting on CTRL-C - waiting for '
                                                        'workers to finish.')
                exitcode = -1
                tar_proc.join()

        d.archiver.close()

        return exitcode


if __name__ == "__main__":
    sys.exit(Shell().main(sys.argv))
