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


from marconi.common import decorators
from marconi.openstack.common import log as logging
from marconi.openstack.common import importutils
from marconi.queues import storage
from marconi.queues.storage.amqp.v1_0 import controllers
from marconi.queues.storage.amqp.v1_0 import options
from marconi.queues.storage.amqp.v1_0.utils import get_host_port, connect_socket
from pyngus import ConnectionEventHandler
from pyngus import Container
import uuid

LOG = logging.getLogger(__name__)

proton = importutils.try_import("proton")


def _connection(conf):
    # Socket for inbound connections
    host, port = get_host_port(conf.uri)
    socket = connect_socket(host, port)

    # Create server container
    container = Container(uuid.uuid4().hex)

    # Create connection
    # Other properties we may include
    # x-trace-protocol, c-ssl-ca-file, idle-time-out
    conn_properties = {'hostname': host}
    connection = Connection(container, socket, host, conn_properties)

    return connection


class DataDriver(storage.DataDriverBase):

    def __init__(self, conf, cache):

        if not proton:
            LOG.error("Required module 'proton' not found.")
            raise ImportError("Failed to import proton module")

        super(DataDriver, self).__init__(conf, cache)

        opts = options.PROTON_OPTIONS

        self.conf.register_opts(opts,
                                group=options.PROTON_GROUP)

        self.proton_conf = self.conf[options.PROTON_GROUP]

    def is_alive(self):
        if ConnectionEventHandler.connection_active(self.connection):
            return True
        else:
            return False

    @decorators.lazy_property(write=False)
    def connection(self):
        """Proton connection instance"""
        return _connection(self.proton_conf)

    @decorators.lazy_property(write=False)
    def queue_controller(self):
        return controllers.QueueController(self)

    @decorators.lazy_property(write=False)
    def message_controller(self):
        return controllers.MessageController(self)


class Connection(ConnectionEventHandler):
    """Connection management"""
    def __init__(self, container, socket, name, properties):
        # NOTE(vkmc): the name must be unique across the entire messaging
        # domain. We could use for this the client-id
        self.socket = socket
        self.connection = container.create_connection(name, self, properties)
        self.connection.user_context = self
        self.connection.pn_sasl.mechanisms("ANONYMOUS")
        self.connection.pn_sasl.client()
        self.connection.open()

        # We could have many sender/receiver links
        # How many would we be using? I assume one for now
        self.sender_links = set()
        self.receiver_links = set()

    def destroy_connection(self):
        for link in self.sender_links.copy():
            link.destroy()
        for link in self.receiver_links.copy():
            link.destroy()
        if self.connection:
            self.connection.close_connection()
            self.connection = None
        if self.socket:
            self.socket.close()
            self.socket = None

    def create_sender(self, source_addr, target_addr):
        sender = self.connection.create_sender(source_addr, target_addr)
        sender.open()
        return sender

    @staticmethod
    def sender_destroy(sender):
        sender.close()

    def create_receiver(self, source_addr, target_addr):
        receiver = self.connection.create_receiver(source_addr, target_addr)
        receiver.open()
        return receiver

    @staticmethod
    def receiver_destroy(receiver):
        receiver.close()