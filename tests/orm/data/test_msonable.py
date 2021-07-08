# -*- coding: utf-8 -*-
"""Tests for the :class:`aiida.orm.nodes.data.msonable.MsonableData` data type."""
from monty.json import MSONable
import pymatgen
import pytest

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
        return cls(d['data'])


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


def test_invalid_class_not_msonable():
    """Test the ``MsonableData`` constructor raises if object does not sublass ``MSONable``."""

    class InvalidClass:
        pass

    with pytest.raises(TypeError, match=r'the `obj` argument needs to implement the ``MSONable`` class.'):
        MsonableData(InvalidClass())


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
    data = {'a': 1}
    obj = MsonableClass(data)
    node = MsonableData(obj)
    node.store()

    assert isinstance(node.obj, MsonableClass)
    assert node.obj.data == data

    loaded = load_node(node.pk)
    assert isinstance(node.obj, MsonableClass)
    assert loaded.obj.data == data


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
