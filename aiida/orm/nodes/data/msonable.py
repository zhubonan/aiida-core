# -*- coding: utf-8 -*-
"""Data plugin for classes that implement the ``MSONable`` class of the ``monty`` library."""
import importlib

from monty.json import MSONable

from aiida.orm import Data


class MsonableData(Data):
    """Data plugin that allows to easily wrap objects that are MSONable.

    To use this class, simply construct it passing an isntance of any ``MSONable`` class and store it, for example:

        from pymatgen.core import Molecule

        molecule = Molecule(['H']. [0, 0, 0])
        node = MsonableData(molecule)
        node.store()

    After storing, the node can be loaded like any other node and the original MSONable instance can be retrieved:

        loaded = load_node(node.pk)
        molecule = loaded.obj

    .. note:: As the ``MSONable`` mixin class requires, the wrapped object needs to implement the methods ``as_dict``
        and ``from_dict``. A default implementation should be present on the ``MSONable`` base class itself, but it
        might need to be overridden in a specific implementation.

    """

    def __init__(self, obj, *args, **kwargs):
        """Construct the node from the pymatgen object."""
        if obj is None:
            raise TypeError('the `obj` argument cannot be `None`.')

        if not isinstance(obj, MSONable):
            raise TypeError('the `obj` argument needs to implement the ``MSONable`` class.')

        for method in ['as_dict', 'from_dict']:
            if not hasattr(obj, method) or not callable(getattr(obj, method)):
                raise TypeError(f'the `obj` argument does not have the required `{method}` method.')

        super().__init__(*args, **kwargs)

        self._obj = obj
        self.set_attribute_many(obj.as_dict())

    def _get_object(self):
        """Return the cached wrapped MSONable object.

        .. note:: If the object is not yet present in memory, for example if the node was loaded from the database,
            the object will first be reconstructed from the state stored in the node attributes.

        """
        try:
            return self._obj
        except AttributeError:
            attributes = self.attributes
            class_name = attributes['@class']
            module_name = attributes['@module']

            try:
                module = importlib.import_module(module_name)
            except ImportError as exc:
                raise ImportError(f'the objects module `{module_name}` can not be imported.') from exc

            try:
                cls = getattr(module, class_name)
            except AttributeError as exc:
                raise ImportError(
                    f'the objects module `{module_name}` does not contain the class `{class_name}`.'
                ) from exc

            self._obj = cls.from_dict(attributes)
            return self._obj

    @property
    def obj(self):
        """Return the wrapped MSONable object."""
        return self._get_object()
