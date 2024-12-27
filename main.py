import asyncio
import time
import json
import resource


class Obj(dict):
    __id_counter = 0
    __id_obj_ref = {}

    def __init__(self, life_time=3600):
        self.__life_time = 3600
        self.__created_time = time.time()
        self.__listeners = set()
        self.__childs = set()
        self.__id = self.__class__.__id_counter
        self.__class__.__id_obj_ref[self.__id] = self
        self.__parent = -1
        
        self.__class__.__id_counter += 1

        super().__init__()

    @property
    def childs(self):
        return self.__childs

    @childs.setter
    def childs(self, childs):
        self.__childs = childs

    @property
    def listeners(self):
        if self.__parent == -1 or self.__id == 0 or len(self.__listeners) > 0:
            return self.__listeners
        return self.parent.listeners()

    @property
    def id(self):
        return self.__id

    @property
    def life_time(self):
        return self.__life_time

    @property
    def created_time(self):
        return self.__created_time

    @property
    def is_alive(self):
        return self.life_time < 0 or self.life_time + self.created_time >= time.time()

    @property
    def parent(self):
        if self.__parent == -1:
            return None
        return get_by_id(self.__parent)
        
    @parent.setter
    def parent(self, value):
        self.__parent = value.id

    @classmethod
    def get_by_id(cls, id):
        return cls.__id_obj_ref.get(id)

    def as_dct(self, child_level=0):
        dct = {"id":self.__id, "parent_id":self.__parent}
        dct.update(self)
        
        if child_level > 0:
            dct.update({"childs": []})
            for child in self.__childs:
                dct["childs"].append(child.as_dct(child_level - 1))
        return dct
    
    def add_child(self, child):
        if child.parent is not None:
            raise Exception("child already has parent")
        child.parent = self
        self.__childs.add(child)

    def remove_child(self, child):
        self.__childs.remove(child)

    def listen(self, client):
        self.__listeners.add(client)

    def unlisten(self, client):
        self.__listeners.remove(client)

    def __del__(self, *args, **kwargs):
        del self.__class__.__id_obj_ref[self.__id]
        #super().__del__(self, *args, **kwargs)

    def __hash__(self, *args, **kwargs):
        return self.__id

    def __eq__(self, other):
        return isinstance(other, Obj) and self.__id == other.__id


class Protocol(asyncio.protocols.Protocol):
    main_obj = Obj(-1)

    def __init__(self):
        self.__transport = None
        self.cmds = {"get": None, "crt": None, "upd": None, "lst": None, "del": None, "sig": None, "mut": None}
        self.cmds["crt"] = self.crt
        self.cmds["get"] = self.get
        self.cmds["lst"] = self.lst
        self.cmds["upd"] = self.upd
        self.cmds["sig"] = self.sig
        self.cmds["mut"] = self.mut
        
    def connection_made(self, transport: asyncio.transports.Transport) -> None:
        self.__transport = transport
        print(transport.get_extra_info("peername"))

    def connection_lost(self, exc) -> None:
        pass

    def eof_received(self) -> None:
        self.__transport.close()

    def pause_writing(self) -> None:
        pass

    def resume_writing(self) -> None:
        pass

    def cmd(self, data):
        client_id = data.get("client_id")
        cmd = data.get("cmd")
        id_ = data.get("id")
        references = data.get("references")
        meta = data.get("meta")
        request_id = data.get("request_id")
        
        if client_id is None:
            raise Exception("client_id is not specified or is Null")
        if id_ is None:
            raise Exception("id is not specified")
        if cmd not in self.cmds:
            raise Exception(f"cmd is not specified or not is {'/'.join(self.cmds.keys())}")
        
        obj = Obj.get_by_id(id_)
        func = self.cmds.get(cmd)

        resp = {"client_id": client_id, "cmd": cmd, "status": True}
        
        if references:
            resp["references"] = references
        if meta:
            resp["meta"] = meta
        if request_id is not None:
            resp["request_id"] = request_id
        
        try:
            resp.update(func(obj, data))
            if resp.get("data") or resp.get("meta"):
                for listener in obj.listeners:
                    if not listener.is_closing() and listener is not self.__transport:
                        listener.write(json.dumps(resp, ensure_ascii=False).encode())
        except Exception as ex:
            resp.update({"error": str(ex), "status": False})
        
        return resp

    def sig(self, obj, data):
        return data

    def crt(self, parent, data):
        new_obj = Obj()
        default_data = data.get("data")
        if default_data:
            new_obj.update(default_data)
        parent.add_child(new_obj)
        return {"id": new_obj.id, "data": new_obj.as_dct()}

    def upd(self, obj, data):
        new_data = data.get("data")
        #if not new_data:
        #    raise Exception("data is not specified")
        if new_data:
            obj.update(new_data)
        return {"id": obj.id, "data": new_data}

    def lst(self, obj, data):
        print(self.__transport.get_extra_info("peername"), "create")
        obj.listen(self.__transport)
        for l in obj.listeners:
            print(l.get_extra_info("peername"), "get")
        return {"id": obj.id}

    def mut(self, obj, data):
        obj.unlisten(self.__transport)
        return {"id": obj.id}

    def get(self, obj, data):
        child_level = data.get("child_level", 0)
        return {"id": obj.id, "data": obj.as_dct(child_level=child_level)}

    def message_handler(self, msg):
        try:
            data = json.loads(msg)
        except:
            return {"error": "failed to process message, message format must be json", "status": False}
        try:
            return self.cmd(data)
        except Exception as ex:
            return {"error": str(ex), "status": False}

    def data_received(self, msg) -> None:
        print("received from", self.__transport.get_extra_info("peername"), msg)
        self.__transport.write(json.dumps(self.message_handler(msg), ensure_ascii=False).encode())

    @classmethod
    def init_garbage_collector(cls):
        async def garbage_collector(cls):
            async def collect(obj):
                if obj is None:
                    obj = cls.main_obj
                removed = set()
                for child in obj.childs:
                    if not obj.is_alive:
                        removed.add(child)
                    else:
                        await collect(child)
                    await asyncio.sleep(0)
                obj.childs = obj.childs.difference(removed)

            while True:
                await collect(cls.main_obj)
                await asyncio.sleep(1)
                
        ioloop = asyncio.get_event_loop()
        ioloop.create_task(garbage_collector(cls))


class Server:
    def __init__(self, host, port, protocol, backlog=10, ioloop=None):
        self._host = host
        self._port = port
        self._protocol = protocol
        self._backlog = backlog
        self._ioloop = ioloop
    
    async def run_forever(self):
        self._ioloop = self._ioloop or asyncio.get_event_loop()
        server = await self._ioloop.create_server(self._protocol, self._host, self._port, 
                                                  backlog=self._backlog, start_serving=False)
        print(f"Serving on {self._host}:{self._port}")
        async with server:
            await server.serve_forever()


async def main():
    resource.setrlimit(resource.RLIMIT_AS, (int(1024 * 20), (1024 * 30))
    
    host = "0.0.0.0"
    port = 15437
    protocol = Protocol
    
    proxy_server = Server(host, port, protocol)

    protocol.init_garbage_collector()
    await proxy_server.run_forever()

asyncio.run(main())
