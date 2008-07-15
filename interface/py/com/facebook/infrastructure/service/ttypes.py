#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

from thrift.Thrift import *
import fb303.ttypes


from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class column_t:

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'columnName', None, None, ), # 1
    (2, TType.STRING, 'value', None, None, ), # 2
    (3, TType.I32, 'timestamp', None, None, ), # 3
  )

  def __init__(self, d=None):
    self.columnName = None
    self.value = None
    self.timestamp = None
    if isinstance(d, dict):
      if 'columnName' in d:
        self.columnName = d['columnName']
      if 'value' in d:
        self.value = d['value']
      if 'timestamp' in d:
        self.timestamp = d['timestamp']

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.columnName = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.value = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.timestamp = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('column_t')
    if self.columnName != None:
      oprot.writeFieldBegin('columnName', TType.STRING, 1)
      oprot.writeString(self.columnName)
      oprot.writeFieldEnd()
    if self.value != None:
      oprot.writeFieldBegin('value', TType.STRING, 2)
      oprot.writeString(self.value)
      oprot.writeFieldEnd()
    if self.timestamp != None:
      oprot.writeFieldBegin('timestamp', TType.I32, 3)
      oprot.writeI32(self.timestamp)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __str__(self):
    return str(self.__dict__)

  def __repr__(self):
    return repr(self.__dict__)

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class batch_mutation_t:

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'table', None, None, ), # 1
    (2, TType.STRING, 'key', None, None, ), # 2
    (3, TType.MAP, 'cfmap', (TType.STRING,None,TType.LIST,(TType.STRUCT,(column_t, column_t.thrift_spec))), None, ), # 3
  )

  def __init__(self, d=None):
    self.table = None
    self.key = None
    self.cfmap = None
    if isinstance(d, dict):
      if 'table' in d:
        self.table = d['table']
      if 'key' in d:
        self.key = d['key']
      if 'cfmap' in d:
        self.cfmap = d['cfmap']

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.table = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.key = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.MAP:
          self.cfmap = {}
          (_ktype1, _vtype2, _size0 ) = iprot.readMapBegin() 
          for _i4 in xrange(_size0):
            _key5 = iprot.readString();
            _val6 = []
            (_etype10, _size7) = iprot.readListBegin()
            for _i11 in xrange(_size7):
              _elem12 = column_t()
              _elem12.read(iprot)
              _val6.append(_elem12)
            iprot.readListEnd()
            self.cfmap[_key5] = _val6
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('batch_mutation_t')
    if self.table != None:
      oprot.writeFieldBegin('table', TType.STRING, 1)
      oprot.writeString(self.table)
      oprot.writeFieldEnd()
    if self.key != None:
      oprot.writeFieldBegin('key', TType.STRING, 2)
      oprot.writeString(self.key)
      oprot.writeFieldEnd()
    if self.cfmap != None:
      oprot.writeFieldBegin('cfmap', TType.MAP, 3)
      oprot.writeMapBegin(TType.STRING, TType.LIST, len(self.cfmap))
      for kiter13,viter14 in self.cfmap.items():
        oprot.writeString(kiter13)
        oprot.writeListBegin(TType.STRUCT, len(viter14))
        for iter15 in viter14:
          iter15.write(oprot)
        oprot.writeListEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __str__(self):
    return str(self.__dict__)

  def __repr__(self):
    return repr(self.__dict__)

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class superColumn_t:

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'name', None, None, ), # 1
    (2, TType.LIST, 'columns', (TType.STRUCT,(column_t, column_t.thrift_spec)), None, ), # 2
  )

  def __init__(self, d=None):
    self.name = None
    self.columns = None
    if isinstance(d, dict):
      if 'name' in d:
        self.name = d['name']
      if 'columns' in d:
        self.columns = d['columns']

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.name = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.LIST:
          self.columns = []
          (_etype19, _size16) = iprot.readListBegin()
          for _i20 in xrange(_size16):
            _elem21 = column_t()
            _elem21.read(iprot)
            self.columns.append(_elem21)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('superColumn_t')
    if self.name != None:
      oprot.writeFieldBegin('name', TType.STRING, 1)
      oprot.writeString(self.name)
      oprot.writeFieldEnd()
    if self.columns != None:
      oprot.writeFieldBegin('columns', TType.LIST, 2)
      oprot.writeListBegin(TType.STRUCT, len(self.columns))
      for iter22 in self.columns:
        iter22.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __str__(self):
    return str(self.__dict__)

  def __repr__(self):
    return repr(self.__dict__)

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class batch_mutation_super_t:

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'table', None, None, ), # 1
    (2, TType.STRING, 'key', None, None, ), # 2
    (3, TType.MAP, 'cfmap', (TType.STRING,None,TType.LIST,(TType.STRUCT,(superColumn_t, superColumn_t.thrift_spec))), None, ), # 3
  )

  def __init__(self, d=None):
    self.table = None
    self.key = None
    self.cfmap = None
    if isinstance(d, dict):
      if 'table' in d:
        self.table = d['table']
      if 'key' in d:
        self.key = d['key']
      if 'cfmap' in d:
        self.cfmap = d['cfmap']

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.table = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.key = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.MAP:
          self.cfmap = {}
          (_ktype24, _vtype25, _size23 ) = iprot.readMapBegin() 
          for _i27 in xrange(_size23):
            _key28 = iprot.readString();
            _val29 = []
            (_etype33, _size30) = iprot.readListBegin()
            for _i34 in xrange(_size30):
              _elem35 = superColumn_t()
              _elem35.read(iprot)
              _val29.append(_elem35)
            iprot.readListEnd()
            self.cfmap[_key28] = _val29
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('batch_mutation_super_t')
    if self.table != None:
      oprot.writeFieldBegin('table', TType.STRING, 1)
      oprot.writeString(self.table)
      oprot.writeFieldEnd()
    if self.key != None:
      oprot.writeFieldBegin('key', TType.STRING, 2)
      oprot.writeString(self.key)
      oprot.writeFieldEnd()
    if self.cfmap != None:
      oprot.writeFieldBegin('cfmap', TType.MAP, 3)
      oprot.writeMapBegin(TType.STRING, TType.LIST, len(self.cfmap))
      for kiter36,viter37 in self.cfmap.items():
        oprot.writeString(kiter36)
        oprot.writeListBegin(TType.STRUCT, len(viter37))
        for iter38 in viter37:
          iter38.write(oprot)
        oprot.writeListEnd()
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __str__(self):
    return str(self.__dict__)

  def __repr__(self):
    return repr(self.__dict__)

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

