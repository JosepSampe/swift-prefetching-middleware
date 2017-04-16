'''
A filter that allows a caching system in the Proxy Server.

@author: josep sampe
'''
import xattr
import logging
import pickle
import errno
import os
from swift.common.utils import get_logger
from swift.common.utils import register_swift_info
from swift.common.exceptions import DiskFileXattrNotSupported
from swift.common.exceptions import DiskFileNoSpace
from swift.common.exceptions import DiskFileNotExist
from swift.common.swob import Request, Response

SWIFT_METADATA_KEY = 'user.swift.metadata'
PICKLE_PROTOCOL = 2


class CachingMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='caching')

        self.register_info()

    def register_info(self):
        register_swift_info('caching',
                            forbidden_chars=self.forbidden_chars,
                            maximum_length=self.maximum_length,
                            forbidden_regexp=self.forbidden_regexp
                            )

    def is_object_in_cache(self, path):
        raise NotImplementedError

    def get_cached_object(self, path):
        raise NotImplementedError

    @property
    def is_object_prefetch(self):
        return 'X-Object-Prefetch' in self.request.headers

    def prefetch_object(self, path):
        raise NotImplementedError

    def __call__(self, env, start_response):
        req = Request(env)
        path = req.path
        # TODO: Handle request

        # Pass on to downstream WSGI component
        return self.app(env, start_response)


class CachingMiddlewareDisk(CachingMiddleware):

    def __init__(self, app, conf):
        super(CachingMiddleware, self).__init__(conf, app)

    def read_metadata(self, fd, obj_path):
        """
        Helper function to read the pickled metadata from an object file.

        :param fd: file descriptor or filename to load the metadata from
        :param filename: full path of the file
        :returns: dictionary of metadata
        """
        metadata = ''
        key = 0
        try:
            while True:
                metadata += xattr.getxattr(fd, '%s%s' % (SWIFT_METADATA_KEY,
                                                         (key or '')))
                key += 1
        except (IOError, OSError) as e:
            if metadata == '':
                return False
            for err in 'ENOTSUP', 'EOPNOTSUPP':
                if hasattr(errno, err) and e.errno == getattr(errno, err):
                    msg = "Filesystem at %s does not support xattr" % \
                          obj_path
                    logging.exception(msg)
                    raise DiskFileXattrNotSupported(e)
            if e.errno == errno.ENOENT:
                raise DiskFileNotExist()
        return pickle.loads(metadata)

    def write_metadata(self, obj_path, metadata, xattr_size=65536):
        """
        Helper function to write pickled metadata for an object file.

        :param obj_path: full path of the file
        :param metadata: metadata to write
        """
        metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
        key = 0
        while metastr:
            try:
                xattr.setxattr(obj_path, '%s%s' % (SWIFT_METADATA_KEY, key or ''),
                               metastr[:xattr_size])
                metastr = metastr[xattr_size:]
                key += 1
            except IOError as e:
                for err in 'ENOTSUP', 'EOPNOTSUPP':
                    if hasattr(errno, err) and e.errno == getattr(errno, err):
                        msg = "Filesystem at %s does not support xattr" % \
                              obj_path
                        logging.exception(msg)
                        raise DiskFileXattrNotSupported(e)
                if e.errno in (errno.ENOSPC, errno.EDQUOT):
                    msg = "No space left on device for %s" % obj_path
                    logging.exception(msg)
                    raise DiskFileNoSpace()
                raise

    def get_object_metadata(self, obj_path):
        """
        Retrieves the swift metadata of a specified data file

        :param data_file: full path of the data file
        :returns: dictionary with all swift metadata
        """
        fd = os.open(obj_path, os.O_RDONLY)
        metadata = self.read_metadata(fd, obj_path)
        os.close(fd)

        return metadata

    def is_object_in_cache(self, path):
        """
        Checks if an object is in cache.
        :return: True/False
        """
        obj_path = "/mnt/data/swift_cache/"+path
        self.logger.info('Checking in cache: ' + path)

        return os.path.isfile(obj_path)

    def get_cached_object(self, path):
        """
        Gets the object from local cache.
        :return: Response object
        """
        obj_path = "/mnt/data/swift_cache/"+path
        self.logger.info('Object %s in cache', path)

        with open(obj_path, 'r') as f:
            data = f.read()

        metadata = self.get_object_metadata(obj_path)
        response = Response(body=data,
                            headers=metadata,
                            request=self.request)
        return response

    def prefetch_object(self, path):
        obj_path = "/mnt/data/swift_cache/"+path
        if self.request.headers['X-Object-Prefetch'] == 'True':
            self.logger.info('Putting into cache '+path)
            new_req = self.request.copy_get()
            new_req.headers['function-enabled'] = False
            response = new_req.get_response(self.app)

            if response.is_success:
                if not os.path.exists(os.path.dirname(obj_path)):
                    os.makedirs(os.path.dirname(obj_path))
                with open(obj_path, 'w') as fn:
                    fn.write(response.body)
                self.set_object_metadata(obj_path, response.headers)

                return Response(body='Prefetched: '+path+'\n', request=self.request)

            else:
                return Response(body='An error was occurred prefetching: '+path+'\n',
                                request=self.request)

        elif self.request.headers['X-Object-Prefetch'] == 'False':
            if os.path.isfile(obj_path):
                os.remove(obj_path)
            return Response(body='Deleting '+path+' from cache\n', request=self.request)


class CachingMiddlewareMemcache(CachingMiddleware):

    def __init__(self, app, conf):
        super(CachingMiddleware, self).__init__(conf, app)

    def is_object_in_cache(self, path):
        """
        Checks if an object is in memcache. If exists, the object is stored
        in self.cached_object.
        :return: True/False
        """
        self.logger.info('Checking in cache: ' + path)
        self.cached_object = self.memcache.get(path)

        return self.cached_object is not None

    def get_cached_object(self, path):
        """
        Gets the object from memcache.
        :return: Response object
        """
        self.logger.info('Object %s in cache', path)
        cached_obj = pickle.loads(self.cached_object)
        resp_headers = cached_obj["Headers"]
        resp_headers['content-length'] = len(cached_obj["Body"])

        response = Response(body=cached_obj["Body"],
                            headers=resp_headers,
                            request=self.request)
        return response

    def prefetch_object(self, path):
        if self.request.headers['X-Object-Prefetch'] == 'True':
            self.logger.info('Putting into cache '+path)
            new_req = self.request.copy_get()
            new_req.headers['function-enabled'] = False
            response = new_req.get_response(self.app)

            cached_obj = {}
            cached_obj['Body'] = response.body
            cached_obj["Headers"] = response.headers

            if response.is_success:
                self.memcache.set(path, pickle.dumps(cached_obj))
                return Response(body='Prefetched: '+path+'\n', request=self.request)
            else:
                return Response(body='An error was occurred prefetcheing: '+path+'\n',
                                request=self.request)

        elif self.request.headers['X-Object-Prefetch'] == 'False':
            self.memcache.delete(path)
            return Response(body='Deleting '+path+' from cache\n', request=self.request)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def name_check_filter(app):
        return CachingMiddleware(app, conf)
    return name_check_filter
