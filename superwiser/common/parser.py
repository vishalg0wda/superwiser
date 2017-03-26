try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from ConfigParser import RawConfigParser


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
    section = section_from_program(program_name)
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
    :returns: section_body(dict)
    """
    result = dict(parsed.items(section))
    result['numprocs'] = int(result.get('numprocs', '1'))
    result['weight'] = float(result.get('weight', '1'))
    return result


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
    for (program_name, numprocs, _) in proc_tuples:
        section = section_from_program(program_name)
        # Extract section from template
        section_body = extract_section(template, section)

        # Apply overrides
        if numprocs > 1:
            section_body['numprocs'] = numprocs
        else:
            section_body.pop('numprocs', None)
        section_body['process_name'] = build_process_name(
            program_name, numprocs)

        # Update section in parsed instance
        update_section(result, section, section_body)

    return result


def extract_programs(parsed):
    """Returns a list of dictionaries corresponding to program configurations
    in supervisor.

    :parsed: ConfigParser instance
    :returns: list of dictionaries
    """
    result = {}
    for section in parsed.sections():
        program_name = program_from_section(section)
        result[program_name] = extract_section(parsed, section)
    return result


def extract_prog_tuples(parsed, proc_key='numprocs'):
    """Iterate over sections in the configuration, extract numprocs if provided
    else set it to 0. Return a list of 2-tuples.

    :parsed: ConfigParser instance
    :proc_key: key used to set numprocs in the config
    :returns: list of 2 tuples
    """
    result = []
    programs = extract_programs(parsed)
    for (program_name, program_body) in programs.items():
        result.append(
            (program_name,
             program_body['numprocs'],
             program_body['weight']))
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
