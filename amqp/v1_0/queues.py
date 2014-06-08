# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Implements the AMQP 1.0 storage controller for queues.

Field Mappings:
    In order to reduce the disk / memory space used,
    field names will be, most of the time, the first
    letter of their long name.
"""

import falcon

from marconi.openstack.common.gettextutils import _
import marconi.openstack.common.log as logging
from marconi.openstack.common import timeutils
from marconi.queues import storage
from marconi.queues.storage import errors


LOG = logging.getLogger(__name__)


class QueueController(storage.Queue):
    """This class is responsible for managing queues.

    Queue operations include CRUD, monitoring, etc.

    Storage driver implementations of this class should
    be capable of handling high workloads and huge
    numbers of queues.
    """

    def __init__(self, *args, **kwargs):
        super(QueueController, self).__init__(*args, **kwargs)

    # NOTE(vkmc): AMQP queues are lazily created and are treated
    # as an attribute of the message. We have some ways to deal with
    # this situation.
    # 1. We could return a HTTP code that indicates that this operation
    # is not implemented - 4xx
    # 2. We could return a HTTP code that indicates that this operation
    # has been successful - 201/204 (created/already exists)

    @abc.abstractmethod
    def list(self, project=None, marker=None,
             limit=DEFAULT_QUEUES_PER_PAGE, detailed=False):
        """Base method for listing queues.

        :param project: Project id
        :param marker: The last queue name
        :param limit: (Default 10) Max number of queues to return
        :param detailed: Whether metadata is included

        :returns: An iterator giving a sequence of queues
            and the marker of the next page.
        """
        # NOTE(vkmc): whereas the idea was pretty good, I'm afraid
        # we shoudln't be changing the expected output and making
        # this particular for AMQP

        #  return falcon.HTTP_200

        raise NotImplementedError

    @abc.abstractmethod
    def get_metadata(self, name, project=None):
        """Base method for queue metadata retrieval.

        :param name: The queue name
        :param project: Project id

        :returns: Dictionary containing queue metadata
        :raises: DoesNotExist
        """
        # return falcon.HTTP_200

        raise NotImplementedError

    @abc.abstractmethod
    def create(self, name, project=None):
        """Base method for queue creation.

        :param name: The queue name
        :param project: Project id
        :returns: True if a queue was created and False
            if it was updated.
        """
        # return falcon.HTTP_204

        raise NotImplementedError

    @abc.abstractmethod
    def exists(self, name, project=None):
        """Base method for testing queue existence.

        :param name: The queue name
        :param project: Project id
        :returns: True if a queue exists and False
            if it does not.
        """
        return True

    @abc.abstractmethod
    def set_metadata(self, name, metadata, project=None):
        """Base method for updating a queue metadata.

        :param name: The queue name
        :param metadata: Queue metadata as a dict
        :param project: Project id
        :raises: DoesNotExist
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, name, project=None):
        """Base method for deleting a queue.

        :param name: The queue name
        :param project: Project id
        """
        # return falcon.HTTP_204
        raise NotImplementedError

    @abc.abstractmethod
    def stats(self, name, project=None):
        """Base method for queue stats.

        :param name: The queue name
        :param project: Project id
        :returns: Dictionary with the
            queue stats
        """
        # return falcon.HTTP_200
        raise NotImplementedError