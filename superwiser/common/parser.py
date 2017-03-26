try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from ConfigParser import RawConfigParser


# ==============================================================================
# Reader based utilities
# ==============================================================================


def program_from_section(section):
    return section.split('program:')[1]


def section_from_program(program_name):
    return "program:{}".format(program_name)


def unparse(parsed):
    dest = StringIO.StringIO()
    parsed.write(dest)
    dest.seek(0)
    return dest.read()


def manipulate_numprocs(parsed, program_name, func):
    section = build_section_from_program(program_name)
    numprocs = 1
    if parsed.has_option(section, 'numprocs'):
        numprocs = int(parsed.get(section, 'numprocs'))

    numprocs = func(numprocs)
    if numprocs <= 0:
        raise Exception('You cannot bring numprocs down to 0!')
    elif numprocs == 1:
        parsed.remove_option(section, 'numprocs')
    if numprocs > 1:
        parsed.set(section, 'numprocs', numprocs)

    return unparse(parsed)

def extract_section(parsed, section):
    """Returns a 2-tuple (section_name, dictionary) constructed
    from a section.

    :parsed: RawConfigParser instance
    :section: Complete section name
    :returns: 2-tuple (program_name, section_body)
    """
    section_body = {k: v for (k, v) in parsed.items(section)}

    return (get_program_from_section(section), section_body)


def list_programs(parsed):
    """Returns a list of dictionaries corresponding to program configurations
    in supervisor.

    :parsed: ConfigParser instance
    :returns: list of dictionaries
    """
    result = []
    for section in parsed.sections():
        result.append(extract_section(parsed, section))

    return result


def list_proc_tuples(parsed, proc_key='numprocs'):
    """Iterate over sections in the configuration, extract numprocs if provided
    else set it to 0. Return a list of 2-tuples.

    :parsed: ConfigParser instance
    :proc_key: key used to set numprocs in the config
    :returns: list of 2 tuples
    """
    result = []
    programs = list_programs(parsed)
    for program in programs:
        numprocs = int(program.get(proc_key, '1'))
        result.append((program['hs_program_name'], numprocs))

    return result


def wrap_content_as_fp(content):
    """Wraps a raw byte stream as a file pointer."""
    fp = StringIO.StringIO(content)
    return fp


def parse_file(path):
    """Parses a file and returns a RawConfigParser instance."""
    parsed = RawConfigParser()
    parsed.read(path)
    return parsed


def parse_content(content):
    """Parses some content and returns a RawConfigParser instance"""
    parsed = RawConfigParser()
    parsed.readfp(wrap_content_as_fp(content))
    return parsed


# ==============================================================================
# Writer based utilities
# ==============================================================================


def build_section_from_program(program):
    return "program:{}".format(program)


def build_process_name(program_name, numprocs):
    """Builds process_name for a program section.

    :program_name: Astonishingly, this is the name of the program.
    :numprocs: Number of procs to run for this program.
    :returns: an auto generated template string
    """
    template_str = "%(program_name)s"
    if numprocs > 1:
        # Include process num identifier
        template_str += "_%(process_num)02d"

    return template_str


def update_section(parsed, section_name, section_body):
    """Create a new section inside parsed instace.

    :parsed: RawConfigParser instance
    :section_name: -
    :section_body: The options that go into the section
    :returns: Updated RawConfigParser instance
    """
    if parsed.has_section(section_name):
        parsed.remove_section(section_name)
    parsed.add_section(section_name)
    for (option, value) in section_body.items():
        # Exclude internally used options(those starting with "hs_")
        if option.startswith('hs_'):
            continue
        parsed.set(section_name, option, value)

    return parsed


def build_conf(proc_tuples, template):
    """Build a configuration iterating over proc_tuples referencing template.

    :proc_tuples: List of (programe_name, numprocs) tuples
    :template: reference configuration template
    :returns: configuration template
    :rtype: RawConfigParser instance
    """
    result = RawConfigParser()
    for (program_name, numprocs) in proc_tuples:
        section = build_section_from_program(program_name)
        # Extract section from template
        section_body = extract_section(template, section)

        # Apply overrides
        if numprocs > 1:
            section_body['numprocs'] = numprocs
        section_body['process_name'] = build_process_name(
            program_name, numprocs)

        # Update section in parsed instance
        update_section(result, section, section_body)

    return result


def calculate_delta(old, new):
    """Calculate added & removed sections between two configs.

    :old: Previous configuration state
    :type: RawConfigParser instance
    :new: New configuration state
    :type: RawConfigParser instance
    :returns: added & removed sections
    :rtype: dict
    """
    old_sections = set(old.sections())
    new_sections = set(new.sections())

    # include only programs
    added_sections = [section for section in new_sections - old_sections
                      if section.startswith('program:')]
    removed_sections = [section for section in old_sections - new_sections
                        if section.startswith('program:')]

    return {
        'added_sections': added_sections,
        'removed_sections': removed_sections,
    }




def merge_confs(conf1, conf2):
    conf1_tups = list_proc_tuples(conf1)
    conf2_tups = list_proc_tuples(conf2)
    conf1_procs = set(ele[0] for ele in conf1_tups)
    conf2_procs = set(ele[0] for ele in conf2_tups)
    result = set()
    for tup in conf1_tups:
        result.add(tup)
    for tup in conf2_tups:
        result.add(tup)
    # override common tups
    for proc in (conf1_procs & conf2_procs):
        conf1_numproc = next(ele[1] for ele in conf1_tups if ele[0] == proc)
        conf2_numproc = next(ele[1] for ele in conf2_tups if ele[0] == proc)
        result.remove(next(ele for ele in result if ele[0] == proc))
        result.add((proc, conf1_numproc + conf2_numproc))

    return list(result)
