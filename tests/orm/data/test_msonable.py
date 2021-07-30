# -*- coding: utf-8 -*-
"""Tests for the :class:`aiida.orm.nodes.data.msonable.MsonableData` data type."""
import datetime
import math
from json import loads, dumps

from monty.json import MSONable, MontyEncoder
import pymatgen
import pytest
import numpy

from aiida.orm import load_node
from aiida.orm.nodes.data.msonable import MsonableData


class MsonableClass(MSONable):
    """Dummy class that implements the ``MSONable interface``."""

    def __init__(self, data):
        """Construct a new object."""
        self._data = data

    @property
    def data(self):
        """Return the data of this instance."""
        return self._data

    def as_dict(self):
        """Represent the object as a JSON-serializable dictionary."""
        return {
            '@module': self.__class__.__module__,
            '@class': self.__class__.__name__,
            'data': self._data,
        }

    @classmethod
    def from_dict(cls, d):
        """Reconstruct an instance from a serialized version."""
        return cls(data=d['data'])


class MsonableClass2(MSONable):
    """Dummy class that implements the ``MSONable interface``."""

    def __init__(self, obj, array, timestamp=None):
        """Construct a new object."""
        self._obj = obj
        self._array = array
        if timestamp is None:
            self._timestamp = datetime.datetime.now()
        else:
            self._timestamp = timestamp

    @property
    def obj(self):
        """Return the data of this instance."""
        return self._obj

    @property
    def array(self):
        """Return the data of this instance."""
        return self._array

    @property
    def timestamp(self):
        """Return the timestamp"""
        return self._timestamp

    def as_dict(self):
        """Represent the object as a JSON-serializable dictionary."""
        return_dict = {
            '@module': self.__class__.__module__,
            '@class': self.__class__.__name__,
            'obj': self.obj.as_dict(),
            'timestamp': loads(dumps(self.timestamp, cls=MontyEncoder)),
            'array': loads(dumps(self._array, cls=MontyEncoder))
        }
        return return_dict


def test_construct():
    """Test the ``MsonableData`` constructor."""
    data = {'a': 1}
    obj = MsonableClass(data)
    node = MsonableData(obj)

    assert isinstance(node, MsonableData)
    assert not node.is_stored


def test_constructor_object_none():
    """Test the ``MsonableData`` constructor raises if object is ``None``."""
    with pytest.raises(TypeError, match=r'the `obj` argument cannot be `None`.'):
        MsonableData(None)


def test_invalid_class_no_as_dict():
    """Test the ``MsonableData`` constructor raises if object does not sublass ``MSONable``."""

    class InvalidClass(MSONable):

        @classmethod
        def from_dict(cls, d):
            pass

    # Remove the ``as_dict`` method from the ``MSONable`` base class because that is currently implemented by default.
    del MSONable.as_dict

    with pytest.raises(TypeError, match=r'the `obj` argument does not have the required `as_dict` method.'):
        MsonableData(InvalidClass())


@pytest.mark.usefixtures('clear_database_before_test')
def test_store():
    """Test storing a ``MsonableData`` instance."""
    data = {'a': 1}
    obj = MsonableClass(data)
    node = MsonableData(obj)
    assert not node.is_stored

    node.store()
    assert node.is_stored


@pytest.mark.usefixtures('clear_database_before_test')
def test_load():
    """Test loading a ``MsonableData`` instance."""
    data = {'a': 1}
    obj = MsonableClass(data)
    node = MsonableData(obj)
    node.store()

    loaded = load_node(node.pk)
    assert isinstance(node, MsonableData)
    assert loaded == node


@pytest.mark.usefixtures('clear_database_before_test')
def test_obj():
    """Test the ``MsonableData.obj`` property."""
    data = [1, float('inf'), float('-inf'), float('nan')]
    obj = MsonableClass(data)
    node = MsonableData(obj)
    node.store()

    assert isinstance(node.obj, MsonableClass)
    assert node.obj.data == data

    loaded = load_node(node.pk)
    assert isinstance(node.obj, MsonableClass)

    for left, right in zip(loaded.obj.data, data):

        # Need this explicit case to compare NaN because of the peculiarity in Python where ``float(nan) != float(nan)``
        if isinstance(left, float) and math.isnan(left):
            assert math.isnan(right)
            continue

        try:
            # This is needed to match numpy arrays
            assert (left == right).all()
        except AttributeError:
            assert left == right


@pytest.mark.usefixtures('clear_database_before_test')
def test_complex_obj():
    """Test the ``MsonableData.obj`` property for a more complex class."""
    data = [1, float('inf'), float('-inf'), float('nan')]
    obj = MsonableClass(data)
    obj2 = MsonableClass2(obj=obj, array=numpy.arange(10))
    node = MsonableData(obj2)
    node.store()

    assert isinstance(node.obj, MsonableClass2)
    assert node.obj.obj.data == data

    loaded = load_node(node.pk)
    assert isinstance(node.obj, MsonableClass2)

    for left, right in zip(loaded.obj.obj.data, data):

        # Need this explicit case to compare NaN because of the peculiarity in Python where ``float(nan) != float(nan)``
        if isinstance(left, float) and math.isnan(left):
            assert math.isnan(right)
            continue

        try:
            # This is needed to match numpy arrays
            assert (left == right).all()
        except AttributeError:
            assert left == right

    assert isinstance(loaded.obj.timestamp, datetime.datetime)
    numpy.testing.assert_allclose(loaded.obj.array, numpy.arange(10))


@pytest.mark.usefixtures('clear_database_before_test')
def test_unimportable_module():
    """Test the ``MsonableData.obj`` property if the associated module cannot be loaded."""
    obj = pymatgen.core.Molecule(['H'], [[0, 0, 0]])
    node = MsonableData(obj)

    # Artificially change the ``@module`` in the attributes so it becomes unloadable
    node.set_attribute('@module', 'not.existing')
    node.store()

    loaded = load_node(node.pk)

    with pytest.raises(ImportError, match='the objects module `not.existing` can not be imported.'):
        _ = loaded.obj


@pytest.mark.usefixtures('clear_database_before_test')
def test_unimportable_class():
    """Test the ``MsonableData.obj`` property if the associated class cannot be loaded."""
    obj = pymatgen.core.Molecule(['H'], [[0, 0, 0]])
    node = MsonableData(obj)

    # Artificially change the ``@class`` in the attributes so it becomes unloadable
    node.set_attribute('@class', 'NonExistingClass')
    node.store()

    loaded = load_node(node.pk)

    with pytest.raises(ImportError, match=r'the objects module `.*` does not contain the class `NonExistingClass`.'):
        _ = loaded.obj
