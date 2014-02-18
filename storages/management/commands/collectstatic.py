import os
import hashlib

from django.conf import settings
from django.contrib.staticfiles.management.commands import collectstatic

AWS_FAST_COLLECTSTATIC = getattr(settings, 'AWS_FAST_COLLECTSTATIC', False)


class Command(collectstatic.Command):
    """
    This management command overrides staticfiles' collectstatic to decide about
    file changes by comparing MD5 signatures instead of last modified times.
    This significantly speeds up the process when using Amazon S3 as a remote
    backend. It's custom behaviour is disabled by default; you explicitly need
    to set AWS_FAST_COLLECTSTATIC to enable.

    The original collectstatic decides if a file has changed based on the file's
    last modified time. Unfortunately, in many deployment scenarios the static
    files will have the last modified time of the deploy, which leads to them
    being updated every time collectstatic is run. This is both slow and can
    become costly.
    When AWS_FAST_COLLECTSTATIC is set, django-storages' collectstatic compares
    based on the MD5 hashes which are part of S3's metadata.

    History
    -------

    This code first surfaced as Django snippet by millarm:
    https://github.com/AGoodId/django-s3-collectstatic/blob/baf18403f629762a63726e976848e98e79766201/django_s3_collectstatic/management/commands/fasts3collectstatic.py

    Issues were fixed and it was packaged by
    Olof Sjobergh <olofsj@gmail.com> as django-s3-collectstatic.
    https://github.com/AGoodId/django-s3-collectstatic/tree/baf18403f629762a63726e976848e98e79766201

    Maik Hoepfel <m@maikhoepfel.de>, on behalf of Tangent Snowball, rewrote it
    to be merged with django-storages.
    """

    def __init__(self, *args, **kwargs):
        """
        Ensure that the storage class preloads the metadata. This is where the
        actual speedup comes from, as one request can pull the file hashes for
        many or all files in the bucket.
        """
        super(Command, self).__init__(*args, **kwargs)
        if AWS_FAST_COLLECTSTATIC and not getattr(self.storage, 'preload_metadata', True):
            self.log('Forcing storage to preload metadata')
            self.storage.preload_metadata = True

    def get_entry_path(self, prefixed_path, storage):
        """
        The common S3 storages have an entries dictionary which can be prefixed
        with a location. Preferably use the internal routines to come up with
        the correct path.
        """
        # S3BotoStorage
        if hasattr(storage, '_normalize_name'):
            return storage._normalize_name(prefixed_path)
        # S3Storage
        elif hasattr(storage, '_clean_name'):
            return storage._clean_name(prefixed_path)
        else:
            self.log(
                'Unknown storage, guessing entry path for %s' % prefixed_path)
            return os.path.join(self.storage.location, prefixed_path)

    def get_remote_hash(self, prefixed_path):
        """
        Gets remote hash of file. Currently only supports S3 storage backends
        """
        entry_path = self.get_entry_path(prefixed_path, self.storage)
        etag = self.storage.entries.get(entry_path).etag
        # drop enclosing quotation marks
        return etag.replace('"', '')

    def get_local_hash(self, path, source_storage, block_size=2**20):
        """
        Gets MD5 hash of local file. Reads in chunks to save on memory.
        https://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
        """
        md5 = hashlib.md5()
        content = source_storage.open(path)
        for chunk in iter(lambda: content.read(block_size), b''):
            md5.update(chunk)
        return md5.hexdigest()

    def delete_file(self, path, prefixed_path, source_storage):
        """
        Checks if the target file should be deleted if it already exists

        Returns True or False
        """
        # hand off to Django's collectstatic straight away if setting isn't set
        if not AWS_FAST_COLLECTSTATIC:
            return super(Command, self).delete_file(
                path, prefixed_path, source_storage)

        if self.storage.exists(prefixed_path):
            try:
                remote_hash = self.get_remote_hash(prefixed_path)
                local_hash = self.get_local_hash(path, source_storage)
            except:
                # can't get the hashes for some reason, let Django decide
                return super(Command, self).delete_file(
                    path, prefixed_path, source_storage)
            else:
                if remote_hash == local_hash:
                    self.log(
                        u"Skipping '%s' (not modified based on hash)" % path)
                    return False

        return True
