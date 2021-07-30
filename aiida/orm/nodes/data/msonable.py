# -*- coding: utf-8 -*-
"""Data plugin for classes that implement the ``MSONable`` class of the ``monty`` library."""
import importlib
import json

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

        for method in ['as_dict', 'from_dict']:
            if not hasattr(obj, method) or not callable(getattr(obj, method)):
                raise TypeError(f'the `obj` argument does not have the required `{method}` method.')

        super().__init__(*args, **kwargs)

        self._obj = obj

        # Serialize the object by calling ``as_dict`` and performing a roundtrip through JSON encoding.
        # This relies on obj.as_dict() giving JSON serializable outputs. The round trip is necessary for
        # constants NaN, inf, -inf which are serialised (by JSONEncoder) as plain strings and kept during
        # the deserialization.
        serialized = json.loads(json.dumps(obj.as_dict()), parse_constant=lambda x: x)

        # Then we apply our own custom serializer that serializes the float constants infinity and nan to a string value
        # which is necessary because the serializer of the ``json`` standard module deserializes to the Python values
        # that can not be written to JSON.
        self.set_attribute_many(serialized)

    @classmethod
    def _deserialize_float_constants(cls, data):
        """Deserialize the contents of a dictionary ``data`` deserializing infinity and NaN string constants.

        The ``data`` dictionary is recursively checked for the ``Infinity``, ``-Infinity`` and ``NaN`` strings, which
        are the Javascript string equivalents to the Python ``float('inf')``, ``-float('inf')`` and ``float('nan')``
        float constants. If one of the strings is encountered, the Python float constant is returned and otherwise the
        original value is returned.
        """
        if isinstance(data, dict):
            return {k: cls._deserialize_float_constants(v) for k, v in data.items()}
        if isinstance(data, list):
            return [cls._deserialize_float_constants(v) for v in data]
        if data == 'Infinity':
            return float('inf')
        if data == '-Infinity':
            return -float('inf')
        if data == 'NaN':
            return float('nan')
        return data

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

            # First we need to deserialize any infinity or nan float string markers that were serialized in the
            # constructor of this node when it was created. There the decoding step in the JSON roundtrip defined a
            # pass-through for the ``parse_constant`` argument, which means that the serialized versions of the float
            # constants (i.e. the strings ``Infinity`` etc.) are not deserialized in the Python float constants. Here we
            # need to first explicit deserialize them. One would think that we could simply let the ``json.loads`` in
            # the following step take care of this, however, since the attributes would first be serialized by the
            # ``json.dumps`` call, the string placeholders would be dumped again to an actual string, which would then
            # no longer be recognized by ``json.loads`` as the Javascript notation of the float constants and so it will
            # leave them as separate strings.
            deserialized = self._deserialize_float_constants(attributes)

            # Finally, reconstruct the original ``MSONable`` class from the fully deserialized data.
            self._obj = cls.from_dict(deserialized)

            return self._obj

    @property
    def obj(self):
        """Return the wrapped MSONable object."""
        return self._get_object()
