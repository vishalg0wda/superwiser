try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from ConfigParser import RawConfigParser


# ==============================================================================
# Reader based utilities
# ==============================================================================

def get_program_from_section(section):
    return section.strip('program:')


def extract_section(parsed, section):
    """Returns a dictionary constructed from a section."""
    d = {k: v for (k, v) in parsed.items(section)}
    # also include the name of the program, strip the "program:" part
    d['_program_name'] = get_program_from_section(section)

    return d


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
        numprocs = int(program.get(proc_key, '0'))
        result.append((program['_program_name'], numprocs))

    return result


def wrap_content_as_fp(content):
    """Wraps a raw byte stream as a file pointer."""
    fp = StringIO(content)
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
    parsed.add_section(section_name)
    for (option, value) in section_body.items():
        # Exclude internally used options(those starting with "_" underscore)
        if option.startswith('_'):
            continue
        parsed.set(section_body, option, value)

    return parsed


def build_conf_from_template(proc_tuples, template):
    """Build a configuration iterating over proc_tuples referencing template.

    :proc_tuples: List of (programe_name, numprocs) tuples
    :template: reference configuration template
    :returns: configuration template
    :rtype: RawConfigParser instance
    """
    result = RawConfigParser()
    for (program_name, numprocs) in proc_tuples:
        section = build_process_name(program_name)
        # Extract section from template
        section_body = extract_section(template, section)

        # Apply overrides
        if numprocs <= 1:
            section_body.pop('numprocs', None)
        section_body['process_name'] = build_process_name(
            program_name, numprocs)

        # Update section in parsed instance
        update_section(result, section, section_body)

    return result
