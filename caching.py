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
        register_swift_info('caching')

    def is_object_in_cache(self):
        raise NotImplementedError

    def get_cached_object(self):
        raise NotImplementedError

    @property
    def is_object_prefetch(self):
        return 'X-Object-Prefetch' in self.req.headers

    def prefetch_object(self):
        raise NotImplementedError

    def __call__(self, env, start_response):
        self.req = Request(env)

        if self.req.method == 'GET':
            if self.is_object_in_cache():
                resp = self.get_cached_object()
                return resp(env, start_response)

        elif self.req.method == 'POST':
            if self.is_object_prefetch:
                resp = self.prefetch_object()
                return resp(env, start_response)

        # Pass on to downstream WSGI component
        return self.app(env, start_response)


class CachingMiddlewareDisk(CachingMiddleware):

    def __init__(self, app, conf):
        super(CachingMiddlewareDisk, self).__init__(app, conf)

        self.location = conf['location']

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

    def write_metadata(self, fd, metadata, obj_path, xattr_size=65536):
        """
        Helper function to write pickled metadata for an object file.

        :param obj_path: full path of the file
        :param metadata: metadata to write
        """
        metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
        key = 0
        while metastr:
            try:
                xattr.setxattr(fd, '%s%s' % (SWIFT_METADATA_KEY, key or ''),
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

    def set_object_metadata(self, obj_path, metadata):
        """
        Sets the swift metadata to the specified data_file

        :param obj_path: full path of the object
        :param metadata: Metadata dictionary
        """
        fd = os.open(obj_path, os.O_WRONLY)
        self.write_metadata(fd, metadata, obj_path)
        os.close(fd)

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

    def is_object_in_cache(self):
        """
        Checks if an object is in cache.
        :return: True/False
        """
        obj_path = self.location+self.req.path
        self.logger.info('Checking in cache: ' + self.req.path)

        return os.path.isfile(obj_path)

    def get_cached_object(self):
        """
        Gets the object from local cache.
        :return: Response object
        """
        obj_path = self.location+self.req.path
        self.logger.info('Object %s in cache', self.req.path)

        with open(obj_path, 'r') as f:
            data = f.read()

        metadata = self.get_object_metadata(obj_path)
        response = Response(body=data,
                            headers=metadata,
                            request=self.req)
        return response

    def prefetch_object(self):
        obj_path = self.location+self.req.path
        if self.req.headers['X-Object-Prefetch'] == 'True':
            self.logger.info('Putting into cache '+self.req.path)
            new_req = self.req.copy_get()
            new_req.headers['function-enabled'] = False
            response = new_req.get_response(self.app)

            if response.is_success:
                if not os.path.exists(os.path.dirname(obj_path)):
                    print obj_path
                    os.makedirs(os.path.dirname(obj_path))
                with open(obj_path, 'w') as fn:
                    fn.write(response.body)
                self.set_object_metadata(obj_path, response.headers)

                return Response(body='Prefetched: '+self.req.path+'\n',
                                request=self.req)
            else:
                return Response(body='An error was occurred prefetching: ' +
                                self.req.path+'\n', request=self.request)

        elif self.req.headers['X-Object-Prefetch'] == 'False':
            if os.path.isfile(obj_path):
                os.remove(obj_path)
            return Response(body='Deleted '+self.req.path+' from cache\n',
                            request=self.req)


class CachingMiddlewareMemcache(CachingMiddleware):

    def __init__(self, app, conf):
        super(CachingMiddlewareMemcache, self).__init__(app, conf)

    def is_object_in_cache(self):
        """
        Checks if an object is in memcache. If exists, the object is stored
        in self.cached_object.
        :return: True/False
        """
        self.logger.info('Checking in cache: ' + self.req.path)
        self.cached_object = self.memcache.get(self.req.path)

        return self.cached_object is not None

    def get_cached_object(self):
        """
        Gets the object from memcache.
        :return: Response object
        """
        self.logger.info('Object %s in cache', self.req.path)
        cached_obj = pickle.loads(self.cached_object)
        resp_headers = cached_obj["Headers"]
        resp_headers['content-length'] = len(cached_obj["Body"])

        response = Response(body=cached_obj["Body"],
                            headers=resp_headers,
                            request=self.request)
        return response

    def prefetch_object(self):
        if self.req.headers['X-Object-Prefetch'] == 'True':
            self.logger.info('Putting into cache '+self.req.path)
            new_req = self.request.copy_get()
            new_req.headers['function-enabled'] = False
            response = new_req.get_response(self.app)

            cached_obj = {}
            cached_obj['Body'] = response.body
            cached_obj["Headers"] = response.headers

            if response.is_success:
                self.memcache.set(self.req.path, pickle.dumps(cached_obj))
                return Response(body='Prefetched: '+self.req.path+'\n',
                                request=self.request)
            else:
                return Response(body='An error was occurred prefetcheing: ' +
                                self.req.path+'\n', request=self.request)

        elif self.req.headers['X-Object-Prefetch'] == 'False':
            self.memcache.delete(self.req.path)
            return Response(body='Deleting '+self.req.path+' from cache\n',
                            request=self.request)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    conf['type'] = local_conf.get('type', 'disk')
    conf['location'] = local_conf.get('location', '/mnt/data/swift_cache')

    def caching_filter(app):
        if conf['type'] == 'disk':
            return CachingMiddlewareDisk(app, conf)
        if conf['type'] == 'memcache':
            return CachingMiddlewareMemcache(app, conf)
    return caching_filter
