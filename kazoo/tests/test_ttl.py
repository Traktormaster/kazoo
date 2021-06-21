import time

from kazoo.testing import KazooTestCase


class KazooTTLTests(KazooTestCase):
    def test_create_ttl_nodes(self):
        client = self.client
        client.create("/ttl-ns")
        client.create("/ttl-ns/a", b"a-v1", ttl=2000)
        client.create("/ttl-ns/b", b"b-v1", ttl=2000)
        client.create("/ttl-ns/c", b"c-v1", ttl=5000)
        assert set(client.get_children("/ttl-ns")) == set(["a", "b", "c"])
        # wait a little, nothing will time out yet
        time.sleep(1.5)
        assert client.get("/ttl-ns/a")[0] == b"a-v1"
        assert client.get("/ttl-ns/b")[0] == b"b-v1"
        assert client.get("/ttl-ns/c")[0] == b"c-v1"
        # update b
        client.set("/ttl-ns/b", b"b-v2")
        # wait until the first will time out
        time.sleep(1.5)
        assert not client.exists("/ttl-ns/a")
        assert client.get("/ttl-ns/b")[0] == b"b-v2"
        assert client.get("/ttl-ns/c")[0] == b"c-v1"
        # b will time out
        time.sleep(1.5)
        assert not client.exists("/ttl-ns/a")
        assert not client.exists("/ttl-ns/b")
        assert client.get("/ttl-ns/c")[0] == b"c-v1"
        # c will time out
        time.sleep(1.5)
        assert set(client.get_children("/ttl-ns")) == set([])
