from operator import itemgetter

from superwiser.common.parser import parse_content


def calculate_load(entity):
    return entity['numprocs'] * entity['weight']


def raccumulate(work, remaining, churned):
    if remaining <= 0:
        return churned
    if not work:
        return churned
    prog, numprocs, weight = work[0]
    load = numprocs * weight
    if load > remaining:
        if weight > remaining:
            return raccumulate(work[1:], remaining, churned)
        elif weight < remaining:
            if churned:
                last_prog, last_numprocs, last_weight = churned.pop()
                if prog == last_prog:
                    churned.append((last_prog, last_numprocs + 1, last_weight))
                else:
                    churned.append((last_prog, last_numprocs, last_weight))
                    churned.append((prog, 1, weight))
            else:
                churned.append((prog, 1, weight))
            work[0] = (prog, numprocs - 1, weight)
            return raccumulate(work, remaining - weight, churned)
        else:
            churned.append((prog, 1, weight))
            return churned
    elif load <= remaining:
        churned.append((prog, numprocs, weight))
        return raccumulate(work[1:], remaining - load, churned)


def split_work(work, across):
    # Calculate and include load
    workload = []
    total_load = 0.0
    for (name, body) in work.items():
        load = calculate_load(body)
        body['load'] = load
        workload.append((name, body))
        total_load += load

    # Accumulate work until mean load is reached
    mean_load = total_load / across
    allotted = []
    # Sort work in descending order of load
    workload = sorted(workload, key=itemgetter('load'), reverse=True)
    accumulated = []
    while workload:

    for (name, body) in workload:
        accumulated_load += body['load']
        if accumulated_load >= mean_load:
            allotted.extend(accumulated)
            accumulated = []
            accumulated_load = 0.0


def distribute_work(conf, toolchains):
    conf = parse_content(conf)
    programs = extract_programs(conf)
