# -*- coding: utf-8 -*-
import sys

from aiida import load_dbenv
from aiida.common.utils import (export_shard_uuid, get_class_string,
                                get_object_from_string, grouper)

__copyright__ = u"Copyright (c), 2015, ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE (Theory and Simulation of Materials (THEOS) and National Centre for Computational Design and Discovery of Novel Materials (NCCR MARVEL)), Switzerland and ROBERT BOSCH LLC, USA. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file"
__version__ = "0.4.1"
__contributors__ = "Andrea Cepellotti, Andrius Merkys, Giovanni Pizzi, Nicolas Mounet"

IMPORTGROUP_TYPE = 'aiida.import'


def deserialize_attributes(attributes_data, conversion_data):
    import datetime
    import pytz

    if isinstance(attributes_data, dict):
        ret_data = {}
        for k, v in attributes_data.iteritems():
            ret_data[k] = deserialize_attributes(v, conversion_data[k])
    elif isinstance(attributes_data, (list, tuple)):
        ret_data = []
        for value, conversion in zip(attributes_data, conversion_data):
            ret_data.append(deserialize_attributes(value, conversion))
    else:
        if conversion_data is None:
            ret_data = attributes_data
        else:
            if conversion_data == 'date':
                ret_data = datetime.datetime.strptime(
                    attributes_data, '%Y-%m-%dT%H:%M:%S.%f').replace(
                    tzinfo=pytz.utc)
            else:
                raise ValueError("Unknown convert_type '{}'".format(
                    conversion_data))

    return ret_data


def deserialize_field(k, v, fields_info, import_unique_ids_mappings,
                      foreign_ids_reverse_mappings):
    import datetime
    import pytz

    try:
        field_info = fields_info[k]
    except KeyError:
        raise ValueError("Unknown field '{}'".format(k))

    if k == 'id' or k == 'pk':
        raise ValueError("ID or PK explicitly passed!")

    requires = field_info.get('requires', None)
    if requires is None:
        # Actual data, no foreign key
        converter = field_info.get('convert_type', None)
        return (k, deserialize_attributes(v, converter))
    else:
        # Foreign field
        # Correctly manage nullable fields
        if v is not None:
            unique_id = import_unique_ids_mappings[requires][v]
            # map to the PK/ID associated to the given entry, in the arrival DB, 
            # rather than in the export DB

            # I store it in the FIELDNAME_id variable, that directly stores the 
            # PK in the remote table, rather than requiring to create Model
            # instances for the foreing relations
            return ("{}_id".format(k),
                    foreign_ids_reverse_mappings[requires][unique_id])
        else:
            return ("{}_id".format(k), None)


def extract_zip(infile, folder, nodes_export_subfolder="nodes",
                silent=False):
    """
    Extract the nodes to be imported from a zip file.

    :param infile: file path
    :param folder: a SandboxFolder, used to extract the file tree
    :param nodes_export_subfolder: name of the subfolder for AiiDA nodes
    :param silent: suppress debug print
    """
    import os
    import zipfile

    if not silent:
        print "READING DATA AND METADATA..."

    try:
        with zipfile.ZipFile(infile, "r") as zip:

            zip.extract(path=folder.abspath,
                   member='metadata.json')
            zip.extract(path=folder.abspath,
                   member='data.json')

            if not silent:
                print "EXTRACTING NODE DATA..."

            for membername in zip.namelist():
                # Check that we are only exporting nodes within
                # the subfolder!
                # TODO: better check such that there are no .. in the
                # path; use probably the folder limit checks
                if not membername.startswith(nodes_export_subfolder+os.sep):
                    continue
                zip.extract(path=folder.abspath,
                            member=membername)
    except zipfile.BadZipfile:
        raise ValueError("The input file format for import is not valid (not"
                         " a zip file)")

            
def extract_tar(infile, folder, nodes_export_subfolder="nodes",
                silent=False):
    """
    Extract the nodes to be imported from a (possibly zipped) tar file.

    :param infile: file path
    :param folder: a SandboxFolder, used to extract the file tree
    :param nodes_export_subfolder: name of the subfolder for AiiDA nodes
    :param silent: suppress debug print
    """
    import os
    import tarfile

    if not silent:
        print "READING DATA AND METADATA..."

    try:
        with tarfile.open(infile, "r:*", format=tarfile.PAX_FORMAT) as tar:

            tar.extract(path=folder.abspath,
                   member=tar.getmember('metadata.json'))
            tar.extract(path=folder.abspath,
                   member=tar.getmember('data.json'))

            if not silent:
                print "EXTRACTING NODE DATA..."

            for member in tar.getmembers():
                if member.isdev():
                    # safety: skip if character device, block device or FIFO
                    print >> sys.stderr, ("WARNING, device found inside the "
                        "import file: {}".format(member.name))
                    continue
                if member.issym() or member.islnk():
                    # safety: in export, I set dereference=True therefore
                    # there should be no symbolic or hard links.
                    print >> sys.stderr, ("WARNING, link found inside the "
                        "import file: {}".format(member.name))
                    continue
                # Check that we are only exporting nodes within
                # the subfolder!
                # TODO: better check such that there are no .. in the
                # path; use probably the folder limit checks
                if not member.name.startswith(nodes_export_subfolder+os.sep):
                    continue
                tar.extract(path=folder.abspath,
                            member=member)
    except tarfile.ReadError:
        raise ValueError("The input file format for import is not valid (1)")


def extract_tree(infile, folder, silent=False):
    """
    Prepare to import nodes from plain file system tree.

    :param infile: path
    :param folder: a SandboxFolder, used to extract the file tree
    :param silent: suppress debug print
    """
    import os

    def add_files(args,path,files):
        folder = args['folder']
        root = args['root']
        for f in files:
            fullpath = os.path.join(path,f)
            if os.path.isfile(fullpath) == False:
                continue
            relpath = os.path.relpath(fullpath,root)
            if os.path.dirname(relpath) != '':
                folder.get_subfolder(os.path.dirname(relpath)+os.sep,create=True)
            folder.insert_path(os.path.abspath(fullpath),relpath)

    os.path.walk(infile,add_files,{'folder': folder,'root': infile})


def extract_cif(infile, folder, nodes_export_subfolder="nodes",
                aiida_export_subfolder="aiida", silent=False):
    """
    Extract the nodes to be imported from a TCOD CIF file. TCOD CIFs,
    exported by AiiDA, may contain an importable subset of AiiDA database,
    which can be imported. This function prepares SandboxFolder with files
    required for import.

    :param infile: file path
    :param folder: a SandboxFolder, used to extract the file tree
    :param nodes_export_subfolder: name of the subfolder for AiiDA nodes
    :param aiida_export_subfolder: name of the subfolder for AiiDA data
        inside the TCOD CIF internal file tree
    :param silent: suppress debug print
    """
    import os
    import urllib2
    import CifFile
    from aiida.common.exceptions import ValidationError
    from aiida.common.utils import md5_file, sha1_file
    from aiida.orm.data.cif import decode_textfield

    values = CifFile.ReadCif(infile)
    values = values[values.keys()[0]] # taking the first datablock in CIF

    for i in range(0,len(values['_tcod_file_id'])-1):
        name = values['_tcod_file_name'][i]
        if not name.startswith(aiida_export_subfolder+os.sep):
            continue
        dest_path = os.path.relpath(name,aiida_export_subfolder)
        if name.endswith(os.sep):
            if not os.path.exists(folder.get_abs_path(dest_path)):
                folder.get_subfolder(folder.get_abs_path(dest_path),create=True)
            continue
        contents = values['_tcod_file_contents'][i]
        if contents == '?' or contents == '.':
            uri = values['_tcod_file_uri'][i]
            if uri is not None and uri != '?' and uri != '.':
                contents = urllib2.urlopen(uri).read()
        encoding = values['_tcod_file_content_encoding'][i]
        if encoding == '.':
            encoding = None
        contents = decode_textfield(contents,encoding)
        if os.path.dirname(dest_path) != '':
            folder.get_subfolder(os.path.dirname(dest_path)+os.sep,create=True)
        with open(folder.get_abs_path(dest_path),'w') as f:
            f.write(contents)
            f.flush()
        md5  = values['_tcod_file_md5sum'][i]
        if md5 is not None:
            if md5_file(folder.get_abs_path(dest_path)) != md5:
                raise ValidationError("MD5 sum for extracted file '{}' is "
                                      "different from given in the CIF "
                                      "file".format(dest_path))
        sha1 = values['_tcod_file_sha1sum'][i]
        if sha1 is not None:
            if sha1_file(folder.get_abs_path(dest_path)) != sha1:
                raise ValidationError("SHA1 sum for extracted file '{}' is "
                                      "different from given in the CIF "
                                      "file".format(dest_path))


def import_data(in_path,ignore_unknown_nodes=False,
                silent=False):
    """
    Import exported AiiDA environment to the AiiDA database.
    If the 'in_path' is a folder, calls export_tree; otherwise, tries to
    detect the compression format (zip, tar.gz, tar.bz2, ...) and calls the 
    correct function.

    :param in_path: the path to a file or folder that can be imported in AiiDA
    """
    import json
    import os
    import tarfile
    import zipfile
    from itertools import chain

    from django.db import transaction
    from django.utils import timezone

    from aiida.orm import Node, Group
    from aiida.common.exceptions import UniquenessError
    from aiida.common.folders import SandboxFolder, RepositoryFolder
    from aiida.backends.djsite.db import models
    from aiida.common.utils import get_class_string, get_object_from_string
    from aiida.common.datastructures import calc_states


    # This is the export version expected by this function
    expected_export_version = '0.1'

    # The name of the subfolder in which the node files are stored
    nodes_export_subfolder = 'nodes'

    # The returned dictionary with new and existing nodes and links
    ret_dict = {}

    ################
    # EXTRACT DATA #
    ################
    # The sandbox has to remain open until the end
    with SandboxFolder() as folder:
        if os.path.isdir(in_path):
            extract_tree(in_path,folder,silent=silent)
        else:
            if tarfile.is_tarfile(in_path):
                extract_tar(in_path,folder,silent=silent,
                            nodes_export_subfolder=nodes_export_subfolder)
            elif zipfile.is_zipfile(in_path):
                extract_zip(in_path,folder,silent=silent,
                            nodes_export_subfolder=nodes_export_subfolder)
            elif os.path.isfile(in_path) and in_path.endswith('.cif'):
                extract_cif(in_path,folder,silent=silent,
                            nodes_export_subfolder=nodes_export_subfolder)
            else:
                raise ValueError("Unable to detect the input file format, it "
                                 "is neither a (possibly compressed) tar file, "
                                 "nor a zip file.")

        try:
            with open(folder.get_abs_path('metadata.json')) as f:
                metadata = json.load(f)

            with open(folder.get_abs_path('data.json')) as f:
                data = json.load(f)
        except IOError as e:
            raise ValueError("Unable to find the file {} in the import "
                             "file or folder".format(e.filename))
    
        ######################
        # PRELIMINARY CHECKS #
        ######################    
        if metadata['export_version'] != expected_export_version:
            raise ValueError("File export version is {}, but I can import only "
                             "version {}".format(metadata['export_version'],
                                                 expected_export_version))

        ##########################################################################
        # CREATE UUID REVERSE TABLES AND CHECK IF I HAVE ALL NODES FOR THE LINKS #
        ##########################################################################    
        linked_nodes = set(chain.from_iterable((l['input'], l['output'])
                                               for l in data['links_uuid']))
        group_nodes = set(chain.from_iterable(data['groups_uuid'].itervalues()))

        # I preload the nodes, I need to check each of them later, and I also
        # store them in a reverse table
        # I break up the query due to SQLite limitations..
        relevant_db_nodes = {}
        for group in grouper(999, linked_nodes):
            relevant_db_nodes.update({n.uuid: n for n in
                                      models.DbNode.objects.filter(uuid__in=group)})

        db_nodes_uuid = set(relevant_db_nodes.keys())
        dbnode_model = get_class_string(models.DbNode)
        import_nodes_uuid = set(v['uuid'] for v in
                                data['export_data'][dbnode_model].values())

        unknown_nodes = linked_nodes.union(group_nodes) - db_nodes_uuid.union(
            import_nodes_uuid)

        if unknown_nodes and not ignore_unknown_nodes:
            raise ValueError(
                "The import file refers to {} nodes with unknown UUID, therefore "
                "it cannot be imported. Either first import the unknown nodes, "
                "or export also the parents when exporting. The unknown UUIDs "
                "are:\n".format(len(unknown_nodes)) +
                "\n".join('* {}'.format(uuid) for uuid in unknown_nodes))

        ###################################
        # DOUBLE-CHECK MODEL DEPENDENCIES #
        ###################################
        # I hardcode here the model order, for simplicity; in any case, this is
        # fixed by the export version
        model_order = [get_class_string(m) for m in
                       (models.DbUser,
                        models.DbComputer,
                        models.DbNode,
                        models.DbGroup,
                       )
        ]

        # Models that do appear in the import file, but whose import is
        # managed manually
        model_manual = [get_class_string(m) for m in
                        (models.DbLink,
                         models.DbAttribute,)
        ]

        all_known_models = model_order + model_manual

        for import_field_name in metadata['all_fields_info']:
            if import_field_name not in all_known_models:
                raise NotImplementedError("Apparently, you are importing a "
                                          "file with a model '{}', but this does not appear in "
                                          "all_known_models!".format(import_field_name))

        for idx, model_name in enumerate(model_order):
            dependencies = []
            for field in metadata['all_fields_info'][model_name].values():
                try:
                    dependencies.append(field['requires'])
                except KeyError:
                    # (No ForeignKey)
                    pass
            for dependency in dependencies:
                if dependency not in model_order[:idx]:
                    raise ValueError("Model {} requires {} but would be loaded "
                                     "first; stopping...".format(model_name,
                                                                 dependency))

        ###################################################
        # CREATE IMPORT DATA DIRECT UNIQUE_FIELD MAPPINGS #
        ###################################################
        import_unique_ids_mappings = {}
        for model_name, import_data in data['export_data'].iteritems():
            if model_name in metadata['unique_identifiers']:
                # I have to reconvert the pk to integer
                import_unique_ids_mappings[model_name] = {
                    int(k): v[metadata['unique_identifiers'][model_name]] for k, v in
                    import_data.iteritems()}

        ###############
        # IMPORT DATA #
        ###############    
        # DO ALL WITH A TRANSACTION
        with transaction.commit_on_success():
            foreign_ids_reverse_mappings = {}
            new_entries = {}
            existing_entries = {}

            # I first generate the list of data
            for model_name in model_order:
                Model = get_object_from_string(model_name)
                fields_info = metadata['all_fields_info'].get(model_name, {})
                unique_identifier = metadata['unique_identifiers'].get(
                    model_name, None)

                new_entries[model_name] = {}
                existing_entries[model_name] = {}

                foreign_ids_reverse_mappings[model_name] = {}

                # Not necessarily all models are exported
                if model_name in data['export_data']:

                    if unique_identifier is not None:
                        import_unique_ids = set(v[unique_identifier] for v in
                                                data['export_data'][model_name].values())

                        relevant_db_entries = {getattr(n, unique_identifier): n
                                               for n in Model.objects.filter(
                            **{'{}__in'.format(unique_identifier):
                                   import_unique_ids})}

                        foreign_ids_reverse_mappings[model_name] = {
                            k: v.pk for k, v in relevant_db_entries.iteritems()}
                        for k, v in data['export_data'][model_name].iteritems():
                            if v[unique_identifier] in relevant_db_entries.keys():
                                # Already in DB
                                existing_entries[model_name][k] = v
                            else:
                                # To be added
                                new_entries[model_name][k] = v
                    else:
                        new_entries[model_name] = data['export_data'][model_name].copy()

            # I import data from the given model
            for model_name in model_order:
                Model = get_object_from_string(model_name)
                fields_info = metadata['all_fields_info'].get(model_name, {})
                unique_identifier = metadata['unique_identifiers'].get(
                    model_name, None)

                for import_entry_id, entry_data in existing_entries[model_name].iteritems():
                    unique_id = entry_data[unique_identifier]
                    existing_entry_id = foreign_ids_reverse_mappings[model_name][unique_id]
                    # TODO COMPARE, AND COMPARE ATTRIBUTES
                    if model_name not in ret_dict:
                        ret_dict[model_name] = { 'new': [], 'existing': [] }
                    ret_dict[model_name]['existing'].append((import_entry_id,
                                                             existing_entry_id))
                    if not silent:
                        print "existing %s: %s (%s->%s)" % (model_name, unique_id,
                                                            import_entry_id,
                                                            existing_entry_id)
                        # print "  `-> WARNING: NO DUPLICITY CHECK DONE!"
                        # CHECK ALSO FILES!

                # Store all objects for this model in a list, and store them
                # all in once at the end.
                objects_to_create = []
                # This is needed later to associate the import entry with the new pk
                import_entry_ids = {}
                for import_entry_id, entry_data in new_entries[model_name].iteritems():
                    unique_id = entry_data[unique_identifier]
                    import_data = dict(deserialize_field(
                        k, v, fields_info=fields_info,
                        import_unique_ids_mappings=import_unique_ids_mappings,
                        foreign_ids_reverse_mappings=foreign_ids_reverse_mappings)
                                       for k, v in entry_data.iteritems())

                    objects_to_create.append(Model(**import_data))
                    import_entry_ids[unique_id] = import_entry_id

                # Before storing entries in the DB, I store the files (if these
                # are nodes). Note: only for new entries!
                if model_name == get_class_string(models.DbNode):
                    if not silent:
                        print "STORING NEW NODE FILES..."
                    for o in objects_to_create:

                        subfolder = folder.get_subfolder(os.path.join(
                            nodes_export_subfolder, export_shard_uuid(o.uuid)))
                        if not subfolder.exists():
                            raise ValueError("Unable to find the repository "
                                             "folder for node with UUID={} in the exported "
                                             "file".format(o.uuid))
                        destdir = RepositoryFolder(
                            section=Node._section_name,
                            uuid=o.uuid)
                        # Replace the folder, possibly destroying existing 
                        # previous folders, and move the files (faster if we
                        # are on the same filesystem, and 
                        # in any case the source is a SandboxFolder) 
                        destdir.replace_with_folder(subfolder.abspath,
                                                    move=True, overwrite=True)

                # Store them all in once; however, the PK are not set in this way...
                Model.objects.bulk_create(objects_to_create)

                # Get back the just-saved entries
                just_saved = dict(Model.objects.filter(
                    **{"{}__in".format(unique_identifier):
                           import_entry_ids.keys()}).values_list(unique_identifier, 'pk'))

                imported_states = []
                if model_name == get_class_string(models.DbNode):
                    if not silent:
                        print "SETTING THE IMPORTED STATES FOR NEW NODES..."
                    # I set for all nodes, even if I should set it only 
                    # for calculations
                    for unique_id, new_pk in just_saved.iteritems():
                        imported_states.append(
                            models.DbCalcState(dbnode_id=new_pk,
                                               state=calc_states.IMPORTED))
                    models.DbCalcState.objects.bulk_create(imported_states)

                # Now I have the PKs, print the info
                # Moreover, set the foreing_ids_reverse_mappings
                for unique_id, new_pk in just_saved.iteritems():
                    import_entry_id = import_entry_ids[unique_id]
                    foreign_ids_reverse_mappings[model_name][unique_id] = new_pk
                    if model_name not in ret_dict:
                        ret_dict[model_name] = { 'new': [], 'existing': [] }
                    ret_dict[model_name]['new'].append((import_entry_id,
                                                        new_pk))

                    if not silent:
                        print "NEW %s: %s (%s->%s)" % (model_name, unique_id,
                                                       import_entry_id,
                                                       new_pk)

                # For DbNodes, we also have to store Attributes!
                if model_name == get_class_string(models.DbNode):
                    if not silent:
                        print "STORING NEW NODE ATTRIBUTES..."
                    for unique_id, new_pk in just_saved.iteritems():
                        import_entry_id = import_entry_ids[unique_id]
                        # Get attributes from import file
                        try:
                            attributes = data['node_attributes'][
                                str(import_entry_id)]
                            attributes_conversion = data[
                                'node_attributes_conversion'][
                                str(import_entry_id)]
                        except KeyError:
                            raise ValueError("Unable to find attribute info "
                                             "for DbNode with UUID = {}".format(
                                unique_id))

                        # Here I have to deserialize the attributes                        
                        deserialized_attributes = deserialize_attributes(
                            attributes, attributes_conversion)
                        models.DbAttribute.reset_values_for_node(
                            dbnode=new_pk,
                            attributes=deserialized_attributes,
                            with_transaction=False)

            if not silent:
                print "STORING NODE LINKS..."
            ## TODO: check that we are not creating input links of an already 
            ##       existing node...
            import_links = data['links_uuid']
            links_to_store = []

            # Needed for fast checks of existing links
            existing_links_raw = models.DbLink.objects.all().values_list(
                'input', 'output', 'label')
            existing_links_labels = {(l[0], l[1]): l[2] for l in existing_links_raw}
            existing_input_links = {(l[1], l[2]): l[0] for l in existing_links_raw}

            dbnode_reverse_mappings = foreign_ids_reverse_mappings[
                get_class_string(models.DbNode)]
            for link in import_links:
                try:
                    in_id = dbnode_reverse_mappings[link['input']]
                    out_id = dbnode_reverse_mappings[link['output']]
                except KeyError:
                    if ignore_unknown_nodes:
                        continue
                    else:
                        raise ValueError("Trying to create a link with one "
                                         "or both unknown nodes, stopping "
                                         "(in_uuid={}, out_uuid={}, "
                                         "label={})".format(link['input'],
                                                            link['output'], link['label']))

                try:
                    existing_label = existing_links_labels[in_id, out_id]
                    if existing_label != link['label']:
                        raise ValueError("Trying to rename an existing link name, "
                                         "stopping (in={}, out={}, old_label={}, "
                                         "new_label={})".format(in_id, out_id,
                                                                existing_label, link['label']))
                        # Do nothing, the link is already in place and has the correct
                        # name
                except KeyError:
                    try:
                        existing_input = existing_input_links[out_id, link['label']]
                        # If existing_input were the correct one, I would have found
                        # it already in the previous step!
                        raise ValueError("There exists already an input link to "
                                         "node {} with label {} but it does not "
                                         "come the expected input {}".format(
                            out_id, link['label'], in_id))
                    except KeyError:
                        # New link    
                        links_to_store.append(models.DbLink(
                            input_id=in_id, output_id=out_id, label=link['label']))
                        if 'aiida.backends.djsite.db.models.DbLink' not in ret_dict:
                            ret_dict['aiida.backends.djsite.db.models.DbLink'] = { 'new': [] }
                        ret_dict['aiida.backends.djsite.db.models.DbLink']['new'].append((in_id,out_id))
    
            # Store new links
            if links_to_store:
                if not silent:
                    print "   ({} new links...)".format(len(links_to_store))

                models.DbLink.objects.bulk_create(links_to_store)
            else:
                if not silent:
                    print "   (0 new links...)"

            if not silent:
                print "STORING GROUP ELEMENTS..."
            import_groups = data['groups_uuid']
            for groupuuid, groupnodes in import_groups.iteritems():
                # TODO: cache these to avoid too many queries
                group = models.DbGroup.objects.get(uuid=groupuuid)
                nodes_to_store = [dbnode_reverse_mappings[node_uuid]
                                  for node_uuid in groupnodes]
                if nodes_to_store:
                    group.dbnodes.add(*nodes_to_store)

            ######################################################
            # Put everything in a specific group
            dbnode_model_name = get_class_string(models.DbNode)
            existing = existing_entries.get(dbnode_model_name, {})
            existing_pk = [foreign_ids_reverse_mappings[
                               dbnode_model_name][v['uuid']]
                           for v in existing.itervalues()]
            new = new_entries.get(dbnode_model_name, {})
            new_pk = [foreign_ids_reverse_mappings[
                          dbnode_model_name][v['uuid']]
                      for v in new.itervalues()]

            pks_for_group = existing_pk + new_pk

            # So that we do not create empty groups
            if pks_for_group:
                # Get an unique name for the import group, based on the
                # current (local) time
                basename = timezone.localtime(timezone.now()).strftime(
                    "%Y%m%d-%H%M%S")
                counter = 0
                created = False
                while not created:
                    if counter == 0:
                        group_name = basename
                    else:
                        group_name = "{}_{}".format(basename, counter)
                    try:
                        group = Group(name=group_name,
                                      type_string=IMPORTGROUP_TYPE).store()
                        created = True
                    except UniquenessError:
                        counter += 1

                # Add all the nodes to the new group
                # TODO: decide if we want to return the group name
                group.add_nodes(models.DbNode.objects.filter(
                    pk__in=pks_for_group))

                if not silent:
                    print "IMPORTED NODES GROUPED IN IMPORT GROUP NAMED '{}'".format(group.name)
            else:
                if not silent:
                    print "NO DBNODES TO IMPORT, SO NO GROUP CREATED"

    if not silent:
        print "*** WARNING: MISSING EXISTING UUID CHECKS!!"
        print "*** WARNING: TODO: UPDATE IMPORT_DATA WITH DEFAULT VALUES! (e.g. calc status, user pwd, ...)"
        print "DONE."

    return ret_dict


import HTMLParser


class HTMLGetLinksParser(HTMLParser.HTMLParser):
    def __init__(self, filter_extension=None):
        """
        If a filter_extension is passed, only links with extension matching
        the given one will be returned.
        """
        self.filter_extension = filter_extension
        self.links = []
        HTMLParser.HTMLParser.__init__(self)

    def handle_starttag(self, tag, attrs):
        """
        Store the urls encountered, if they match the request.
        """
        if tag == 'a':
            for k, v in attrs:
                if k == 'href':
                    if (self.filter_extension is None or
                            v.endswith('.{}'.format(self.filter_extension))):
                        self.links.append(v)

    def get_links(self):
        """
        Return the links that were found during the parsing phase.
        """
        return self.links


def get_valid_import_links(url):
    """
    Open the given URL, parse the HTML and return a list of valid links where
    the link file has a .aiida extension.
    """
    import urllib2
    import urlparse

    request = urllib2.urlopen(url)
    parser = HTMLGetLinksParser(filter_extension='aiida')
    parser.feed(request.read())

    return_urls = []

    for link in parser.get_links():
        return_urls.append(urlparse.urljoin(request.geturl(), link))

    return return_urls


def serialize_field(data, track_conversion=False):
    """
    Serialize a single field.
    
    :todo: Generalize such that it the proper function is selected also during
        import
    """
    import datetime
    import pytz

    if isinstance(data, dict):
        if track_conversion:
            ret_data = {}
            ret_conversion = {}
            for k, v in data.iteritems():
                ret_data[k], ret_conversion[k] = serialize_field(
                    data=v, track_conversion=track_conversion)
        else:
            ret_data = {k: serialize_field(data=v,
                                           track_conversion=track_conversion)
                        for k, v in data.iteritems()}
    elif isinstance(data, (list, tuple)):
        if track_conversion:
            ret_data = []
            ret_conversion = []
            for value in data:
                this_data, this_conversion = serialize_field(
                    data=value, track_conversion=track_conversion)
                ret_data.append(this_data)
                ret_conversion.append(this_conversion)
        else:
            ret_data = [serialize_field(
                data=value, track_conversion=track_conversion)
                        for value in data]
    elif isinstance(data, datetime.datetime):
        # Note: requires timezone-aware objects!
        ret_data = data.astimezone(pytz.utc).strftime(
            '%Y-%m-%dT%H:%M:%S.%f')
        ret_conversion = 'date'
    else:
        ret_data = data
        ret_conversion = None

    if track_conversion:
        return (ret_data, ret_conversion)
    else:
        return ret_data


def serialize_dict(datadict, remove_fields=[], rename_fields={},
                   track_conversion=False):
    """
    Serialize the dict using the serialize_field function to serialize
    each field.
    
    :param remove_fields: a list of strings. 
      If a field with key inside the remove_fields list is found, 
      it is removed from the dict.
      
      This is only used at level-0, no removal
      is possible at deeper levels.
    
    :param rename_fields: a dictionary in the format
      ``{"oldname": "newname"}``. 

      If the "oldname" key is found, it is replaced with the
      "newname" string in the output dictionary.
      
      This is only used at level-0, no renaming
      is possible at deeper levels.
    :param track_conversion: if True, a tuple is returned, where the first
      element is the serialized dictionary, and the second element is a
      dictionary with the information on the serialized fields.
    """
    ret_dict = {}

    conversions = {}

    for k, v in datadict.iteritems():
        if k not in remove_fields:
            # rename_fields.get(k,k): use the replacement if found in rename_fields,
            # otherwise use 'k' as the default value.
            if track_conversion:
                (ret_dict[rename_fields.get(k, k)],
                 conversions[rename_fields.get(k, k)]) = serialize_field(
                    data=v, track_conversion=track_conversion)
            else:
                ret_dict[rename_fields.get(k, k)] = serialize_field(
                    data=v, track_conversion=track_conversion)

    if track_conversion:
        return (ret_dict, conversions)
    else:
        return ret_dict


def get_all_fields_info():
    """
    Retrieve automatically the information on the fields and store them in a
    dictionary, that will be also stored in the export data, in the metadata
    file.
    
    :return: a tuple with two elements, the all_fiekds_info dictionary, and the
      unique_identifiers dictionary.
    """
    import importlib

    import django.db.models.fields as djf
    import django_extensions

    from aiida.backends.djsite.db import models

    all_fields_info = {}

    user_model_string = get_class_string(models.DbUser)

    # TODO: These will probably need to have a default value in the IMPORT!
    # TODO: maybe define this inside the Model!
    all_exclude_fields = {
        user_model_string: ['password', 'is_staff',
                            'is_superuser', 'is_active',
                            'last_login', 'date_joined'],
    }

    # I start only with DbNode
    export_models = set([get_class_string(Model) for Model in
                         [models.DbNode, models.DbAttribute,
                          models.DbLink, models.DbGroup]])

    while True:
        missing_models = export_models - set(all_fields_info.keys())
        if not missing_models:
            break

        for model_name in missing_models:
            Model = get_object_from_string(model_name)

            thisinfo = {}
            exclude_fields = all_exclude_fields.get(model_name, [])
            for field in Model._meta.fields:
                if field.name in exclude_fields:
                    continue
                if isinstance(field, djf.AutoField):
                    # Do not explicitly store the ID field
                    pass
                elif isinstance(field, (djf.CharField, djf.TextField,
                                        djf.IntegerField, djf.FloatField,
                                        djf.BooleanField, djf.NullBooleanField,
                                        django_extensions.db.fields.UUIDField)):
                    thisinfo[field.name] = {}
                elif isinstance(field, djf.DateTimeField):
                    # This information is needed on importing
                    thisinfo[field.name] = {'convert_type': 'date'}
                elif isinstance(field, django_extensions.db.fields.UUIDField):
                    thisinfo[field.name] = {}
                elif isinstance(field, djf.related.ForeignKey):
                    rel_model_name = get_class_string(field.rel.to)
                    related_name = field.rel.related_name
                    thisinfo[field.name] = {
                        # The 'values' method will return the id (an integer),
                        # so no custom serializer is required
                        'requires': rel_model_name,
                        'related_name': related_name,
                    }
                    export_models.add(rel_model_name)
                else:
                    raise NotImplementedError(
                        "Export not implemented for field of type {}.{}".format(
                            get_class_string(field)))
                all_fields_info[model_name] = thisinfo

    unique_identifiers = {}
    for k in all_fields_info:
        if k == user_model_string:
            unique_identifiers[k] = 'email'
            continue

        # No unique identifier in this case
        if k in [get_class_string(models.DbAttribute),
                 get_class_string(models.DbLink),
                 get_class_string(models.DbExtra)]:
            unique_identifiers[k] = None
            continue

        m = get_object_from_string(k)
        field_names = [f.name for f in m._meta.fields]
        if 'uuid' in field_names:
            unique_identifiers[k] = 'uuid'
        else:
            raise ValueError("Unable to identify the unique identifier "
                             "for model {}".format(k))

    return all_fields_info, unique_identifiers


def export_tree(what, folder, also_parents = True, also_calc_outputs=True,
                allowed_licenses=None, forbidden_licenses=None,
                silent=False):
    """
    Export the DB entries passed in the 'what' list to a file tree.
    
    :todo: limit the export to finished or failed calculations.
    
    :param what: a list of Django database entries; they can belong to different
      models.
    :param folder: a :py:class:`Folder <aiida.common.folders.Folder>` object
    :param also_parents: if True, also all the parents are stored (from th
      DbPath transitive closure table)
    :param also_calc_outputs: if True, any output of a calculation is also exported
    :param allowed_licenses: a list or a function. If a list, then checks
      whether all licenses of Data nodes are in the list. If a function,
      then calls function for licenses of Data nodes expecting True if
      license is allowed, False otherwise.
    :param forbidden_licenses: a list or a function. If a list, then checks
      whether all licenses of Data nodes are in the list. If a function,
      then calls function for licenses of Data nodes expecting True if
      license is allowed, False otherwise.
    :param silent: suppress debug prints
    :raises LicensingException: if any node is licensed under forbidden
      license
    """
    import json
    import os
    import operator
    from collections import defaultdict

    from django.db.models import Q

    import aiida
    from aiida.backends.djsite.db import models
    from aiida.orm import Node, Calculation, load_node
    from aiida.orm.data import Data
    from aiida.common.exceptions import LicensingException
    from aiida.common.folders import RepositoryFolder

    if not silent:
        print "STARTING EXPORT..."

    EXPORT_VERSION = '0.1'
    
    all_fields_info, unique_identifiers = get_all_fields_info()

    entries_ids_to_add = defaultdict(list)
    # I store a list of the actual dbnodes
    groups_entries = []
    group_class_string = get_class_string(models.DbGroup)
    for entry in what:
        class_string = get_class_string(entry)
        entries_ids_to_add[class_string].append(entry.pk)
        if class_string == group_class_string:
            groups_entries.append(entry)

    if also_parents:
        # It is a defaultdict, it will provide an empty list
        given_nodes = entries_ids_to_add[get_class_string(models.DbNode)]

        if given_nodes:
            # Alsof add the parents (to any level) to the query
            given_nodes = list(set(given_nodes +
                                   list(models.DbNode.objects.filter(
                                       children__in=given_nodes).values_list('pk', flat=True))))
            entries_ids_to_add[get_class_string(models.DbNode)] = given_nodes

    if also_calc_outputs:
        given_nodes = entries_ids_to_add[get_class_string(models.DbNode)]

        if given_nodes:
            # Add all (direct) outputs of a calculation object that was already
            # selected
            given_nodes = list(set(given_nodes +
                                   list(models.DbNode.objects.filter(
                                       inputs__pk__in=given_nodes,
                                       inputs__type__startswith=Calculation._query_type_string
                                   ).values_list('pk', flat=True)
                                   )))
            entries_ids_to_add[get_class_string(models.DbNode)] = given_nodes

    # Initial query to fire the generation of the export data
    entries_to_add = {k: [Q(id__in=v)] for k, v
                      in entries_ids_to_add.iteritems()}

    # Check the licenses of exported data.
    if allowed_licenses is not None or forbidden_licenses is not None:
        from inspect import isfunction

        node_licenses = list(aiida.backends.djsite.db.models.DbNode.objects.filter(
            reduce(operator.and_, entries_to_add['aiida.backends.djsite.db.models.DbNode']),
            dbattributes__key='source.license').values_list('pk', 'dbattributes__tval'))
        for pk, license in node_licenses:
            if allowed_licenses is not None:
                try:
                    if isfunction(allowed_licenses):
                        try:
                            if not allowed_licenses(license):
                                raise LicensingException
                        except Exception as e:
                            raise LicensingException
                    else:
                        if license not in allowed_licenses:
                            raise LicensingException
                except LicensingException:
                    raise LicensingException("Node {} is licensed "
                                             "under {} license, which "
                                             "is not in the list of "
                                             "allowed licenses".format(
                                              pk, license))
            if forbidden_licenses is not None:
                try:
                    if isfunction(forbidden_licenses):
                        try:
                            if forbidden_licenses(license):
                                raise LicensingException
                        except Exception as e:
                            raise LicensingException
                    else:
                        if license in forbidden_licenses:
                            raise LicensingException
                except LicensingException:
                    raise LicensingException("Node {} is licensed "
                                             "under {} license, which "
                                             "is in the list of "
                                             "forbidden licenses".format(
                                              pk, license))

    ############################################################
    ##### Start automatic recursive export data generation #####
    ############################################################
    if not silent:
        print "STORING DATABASE ENTRIES..."
    export_data = {}
    while entries_to_add:
        new_entries_to_add = {}
        for model_name, querysets in entries_to_add.iteritems():
            if not silent:
                print "  - Model: {}".format(model_name)
            Model = get_object_from_string(model_name)

            ## Before I was doing this. But it is VERY slow! E.g. 
            ## To get the user owning 44 nodes or 1 group was taking
            ## 26 seconds, while it was taking only 0.1 seconds if the two
            ## queries were run independently!
            ## I think this was doing the wrong type of UNION
            #dbentries = Model.objects.filter(
            #    reduce(operator.or_, querysets)).distinct()
            ## Now I instead create the list of UUIDs and do a set() instead
            ## of .distinct(); then I get the final results with a further
            ## query.
            db_ids = set()
            for queryset in querysets:
                db_ids.update(Model.objects.filter(queryset).values_list(
                    'id', flat=True))
            dbentries = Model.objects.filter(id__in=db_ids)
            entryvalues = dbentries.values(
                'id', *all_fields_info[model_name].keys()
            )

            # Only serialize new nodes (also to avoid infinite loops)
            if model_name in export_data:
                serialized = {
                    str(v['id']): serialize_dict(v, remove_fields=['id'])
                    for v in entryvalues
                    if v['id'] not in export_data[model_name]
                }
            else:
                serialized = {
                    str(v['id']): serialize_dict(v, remove_fields=['id'])
                    for v in entryvalues
                }

            try:
                export_data[model_name].update(serialized)
            except KeyError:
                export_data[model_name] = serialized

            if serialized:
                foreign_fields = {k: v for k, v in
                                  all_fields_info[model_name].iteritems()
                                  if 'requires' in v}

                for k, v in foreign_fields.iteritems():
                    related_queryobj = Q(**{'{}__in'.format(v['related_name']):
                                                serialized.keys()})
                    try:
                        new_entries_to_add[v['requires']].append(related_queryobj)
                    except KeyError:
                        new_entries_to_add[v['requires']] = [related_queryobj]

        entries_to_add = new_entries_to_add

    ######################################
    # Manually manage links and attributes
    ######################################
    # I use .get because there may be no nodes to export
    all_nodes_pk = export_data.get(get_class_string(models.DbNode), {}).keys()
    if sum(len(model_data) for model_data in export_data.values()) == 0:
        if not silent:
            print "No nodes to store, exiting..."
        return

    if not silent:
        print "Exporting a total of {} db entries, of which {} nodes.".format(
            sum(len(model_data) for model_data in export_data.values()),
            len(all_nodes_pk))
    all_nodes_query = models.DbNode.objects.filter(pk__in=all_nodes_pk)

    ## ATTRIBUTES
    if not silent:
        print "STORING NODE ATTRIBUTES..."
    node_attributes = {}
    node_attributes_conversion = {}
    for n in all_nodes_query:
        (node_attributes[str(n.pk)],
         node_attributes_conversion[str(n.pk)]) = serialize_dict(
            n.attributes, track_conversion=True)
    ## If I want to store them 'raw'; it is faster, but more error prone and
    ## less version-independent, I think. Better to optimize the n.attributes 
    ## call.
    # all_nodes_query = models.DbNode.objects.filter(pk__in=all_nodes_pk)
    #node_attributes_raw = list(models.DbAttribute.objects.filter(
    #    dbnode__in=all_nodes_pk).distinct().values(
    #    'bval', 'tval', 'ival', 'fval', 'dval',
    #    'datatype', 'time', 'dbnode', 'key')

    if not silent:
        print "STORING NODE LINKS..."
    ## All 'parent' links (in this way, I can automatically export a node 
    ## that will get automatically attached to a parent node in the end DB,
    ## if the parent node is already present in the DB)
    linksquery = models.DbLink.objects.filter(
        output__in=all_nodes_query).distinct()

    links_uuid = [
        serialize_dict(l, rename_fields={
            'input__uuid': 'input',
            'output__uuid': 'output'})
        for l in linksquery.values(
            'input__uuid', 'output__uuid', 'label')]

    if not silent:
        print "STORING GROUP ELEMENTS..."
    groups_uuid = {g.uuid: list(g.dbnodes.values_list('uuid', flat=True))
                   for g in groups_entries}

    ######################################
    # Now I store
    ######################################    
    # subfolder inside the export package
    nodesubfolder = folder.get_subfolder('nodes',create=True,
                                         reset_limit=True)

    if not silent:
        print "STORING DATA..."
    
    with folder.open('data.json', 'w') as f:
        json.dump({
                'node_attributes': node_attributes,
                'node_attributes_conversion': node_attributes_conversion,
                'export_data': export_data,
                'links_uuid': links_uuid,
                'groups_uuid': groups_uuid,
                }, f)

    metadata = {
        'aiida_version': aiida.get_version(),
        'export_version': EXPORT_VERSION,
        'all_fields_info': all_fields_info,
        'unique_identifiers': unique_identifiers,
        }

    with folder.open('metadata.json', 'w') as f:
        json.dump(metadata, f)

    if silent is not True:
        print "STORING FILES..."

    # Large speed increase by not getting the node itself and looping in memory
    # in python, but just getting the uuid
    for uuid in models.DbNode.objects.filter(pk__in=all_nodes_pk).values_list(
        'uuid', flat=True):
        sharded_uuid = export_shard_uuid(uuid)

        # Important to set create=False, otherwise creates
        # twice a subfolder. Maybe this is a bug of insert_path??

        thisnodefolder = nodesubfolder.get_subfolder(
            sharded_uuid, create=False,
            reset_limit=True)
        # In this way, I copy the content of the folder, and not the folder
        # itself
        thisnodefolder.insert_path(src=RepositoryFolder(
            section=Node._section_name, uuid=uuid).abspath,
                                   dest_name='.')


class MyWritingZipFile(object):
    def __init__(self, zipfile, fname):
        
        self._zipfile = zipfile
        self._fname = fname
        self._buffer = None
        
    def open(self):
        import StringIO
        
        if self._buffer is not None:
            raise IOError("Cannot open again!")
        self._buffer = StringIO.StringIO()

    def write(self, data):
        self._buffer.write(data)

    def close(self):
        self._buffer.seek(0)
        self._zipfile.writestr(self._fname, self._buffer.read())
        self._buffer = None
        
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class ZipFolder(object):
    """
    To improve: if zipfile is closed, do something
    (e.g. add explicit open method, rename open to openfile,
    set _zipfile to None, ...)
    """
    def __init__(self, zipfolder_or_fname, mode=None, subfolder='.', 
                  use_compression=True):
        """
        :param zipfolder_or_fname: either another ZipFolder instance,
          of which you want to get a subfolder, or a filename to create.
        :param mode: the file mode; see the zipfile.ZipFile docs for valid
          strings. Note: can be specified only if zipfolder_or_fname is a
          string (the filename to generate)
        :param subfolder: the subfolder that specified the "current working
          directory" in the zip file. If zipfolder_or_fname is a ZipFolder,
          subfolder is a relative path from zipfolder_or_fname.subfolder
        :param use_compression: either True, to compress files in the Zip, or
          False if you just want to pack them together without compressing.
          It is ignored if zipfolder_or_fname is a ZipFolder isntance.  
        """
        import zipfile
        import os
        
        if isinstance(zipfolder_or_fname, basestring):
            the_mode = mode
            if the_mode is None:
                the_mode = "r"
            if use_compression:
                compression = zipfile.ZIP_DEFLATED
            else:
                compression = zipfile.ZIP_STORED
            self._zipfile = zipfile.ZipFile(zipfolder_or_fname, mode=the_mode,
                                            compression=compression)
            self._pwd = subfolder
        else:
            if mode is not None:
                raise ValueError("Cannot specify 'mode' when passing a ZipFolder")
            self._zipfile = zipfolder_or_fname._zipfile
            self._pwd = os.path.join(zipfolder_or_fname.pwd, subfolder)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()    
            
    def close(self):
        self._zipfile.close()
            
    @property
    def pwd(self):
        return self._pwd

    def open(self, fname, mode='r'):
        if mode == 'w':
            return MyWritingZipFile(
                zipfile=self._zipfile, fname=self._get_internal_path(fname))
        else:
            return self._zipfile.open(self._get_internal_path(fname), mode)

    def _get_internal_path(self, filename):
        import os
        return os.path.normpath(os.path.join(self.pwd, filename))
        
    def get_subfolder(self, subfolder, create=False, reset_limit=False):
        # reset_limit: ignored
        # create: ignored, for the time being
        subfolder = ZipFolder(self, subfolder=subfolder)
        return subfolder

    def insert_path(self, src, dest_name=None, overwrite=True):
        import os

        if dest_name is None:
            base_filename = unicode(os.path.basename(src))
        else:
            base_filename = unicode(dest_name)

        base_filename = self._get_internal_path(base_filename)
        
        if not isinstance(src, unicode):
            src = unicode(src)

        if not os.path.isabs(src):
            raise ValueError("src must be an absolute path in insert_file")

        if not overwrite:
            try:
                self._zipfile.getinfo(filename)
                exists = True
            except KeyError:
                exists = False
            if exists:
                raise IOError("destination already exists: {}".format(
                        filename))

        #print src, filename
        if os.path.isdir(src):
            for dirpath, dirnames, filenames in os.walk(src):
                relpath = os.path.relpath(dirpath, src)
                for fn in dirnames + filenames:
                    real_src = os.path.join(dirpath,fn)
                    real_dest = os.path.join(base_filename,relpath,fn)
                    self._zipfile.write(real_src,
                                        real_dest)
        else:
            self._zipfile.write(src, base_filename)

        
def export_zip(what, outfile = 'testzip', overwrite = False,
              silent = False, use_compression = True, **kwargs):
    import os

    if not overwrite and os.path.exists(outfile):
        raise IOError("The output file '{}' already "
                      "exists".format(outfile))

    import time
    t = time.time()
    with ZipFolder(outfile, mode='w', use_compression = use_compression) as folder:
        export_tree(what, folder=folder, silent=silent, **kwargs)
    if not silent:
        print "File written in {:10.3g} s.".format(time.time() - t)


def export(what, outfile = 'export_data.aiida.tar.gz', overwrite = False,
           silent = False, **kwargs):
    """
    Export the DB entries passed in the 'what' list on a file.
    
    :todo: limit the export to finished or failed calculations.
    
    :param what: a list of Django database entries; they can belong to different
      models.
    :param also_parents: if True, also all the parents are stored (from th
      DbPath transitive closure table)
    :param also_calc_outputs: if True, any output of a calculation is also exported
    :param outfile: the filename of the file on which to export
    :param overwrite: if True, overwrite the output file without asking.
        if False, raise an IOError in this case.
    :param silent: suppress debug print
    
    :raise IOError: if overwrite==False and the filename already exists.
    """
    import os
    import tarfile
    import time
    
    from aiida.common.folders import SandboxFolder

    if not overwrite and os.path.exists(outfile):
        raise IOError("The output file '{}' already "
                      "exists".format(outfile))

    folder = SandboxFolder()
    t1 = time.time()
    export_tree(what, folder=folder, silent=silent, **kwargs)
    t2 = time.time()
    
    if not silent:
        print "COMPRESSING..."

    # PAX_FORMAT: virtually no limitations, better support for unicode
    #   characters
    # dereference=True: at the moment, we should not have any symlink or
    #   hardlink in the AiiDA repository; therefore, do not store symlinks
    #   or hardlinks, but store the actual destinations.
    #   This also simplifies the checks on import.
    t3 = time.time()
    with tarfile.open(outfile, "w:gz", format=tarfile.PAX_FORMAT,
                      dereference=True) as tar:
        tar.add(folder.abspath, arcname="")

        #        import shutil
        #        shutil.make_archive(outfile, 'zip', folder.abspath)#, base_dir='aiida')
    t4 = time.time()

    if not silent:
        filecr_time = t2-t1
        filecomp_time = t4-t3
        print "Exported in {:6.2g}s, compressed in {:6.2g}s, total: {:6.2g}s.".format(filecr_time, filecomp_time, filecr_time + filecomp_time)
        
    if not silent:
        print "DONE."


# Following code: to serialize the date directly when dumping into JSON.
# In our case, it is better to have a finer control on how to parse fields.

# def default_jsondump(data):
#    import datetime 
#
#    if isinstance(data, datetime.datetime):
#        return data.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
#    
#    raise TypeError(repr(data) + " is not JSON serializable")
#with open('testout.json', 'w') as f:
#    json.dump({
#            'entries': serialized_entries,             
#        },
#        f,
#        default=default_jsondump)