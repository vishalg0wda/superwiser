import math
import random
from copy import deepcopy

from superwiser.common.parser import parse_content, extract_prog_tuples
from superwiser.common.log import logger


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
            if (numprocs - 1) == 0:
                work = work[1:]
            else:
                work[0] = (prog, numprocs - 1, weight)
            return raccumulate(work, remaining - weight, churned)
        else:
            last_prog, last_numprocs, last_weight = churned.pop()
            if prog == last_prog:
                churned.append((last_prog, last_numprocs + 1, last_weight))
            else:
                churned.append((last_prog, last_numprocs, last_weight))
                churned.append((prog, 1, weight))
            return churned
    elif load <= remaining:
        churned.append((prog, numprocs, weight))
        return raccumulate(work[1:], remaining - load, churned)


def deduct(offload, workload):
    new_workload = []
    for (prog, numprocs, weight) in workload:
        try:
            numprocs -= next(ele for ele in offload if ele[0] == prog)[1]
            if numprocs > 0:
                new_workload.append((prog, numprocs, weight))
        except StopIteration:
            new_workload.append((prog, numprocs, weight))

    return sorted(new_workload, key=lambda e: e[1] * e[2], reverse=True)


def toggle_ceils():
    has_ceiled = False
    load = yield
    while True:
        if has_ceiled:
            load = math.floor(load)
            has_ceiled = False
        else:
            load = math.ceil(load)
            has_ceiled = True
        load = yield load


def split_work(work, across):
    allotted = []
    if across == 0:
        logger.info('Nothing to split work across')
        return allotted
    total_load = sum(ele[1] * ele[2] for ele in work)
    mean_load = total_load / across
    # Sort work in descending order of load
    work = sorted(work, key=lambda e: e[1] * e[2], reverse=True)
    # Toggle ceiling of mean loads
    toggler = toggle_ceils()
    toggler.send(None)
    while True:
        # Assemble a mean portion of work
        split = raccumulate(deepcopy(work), toggler.send(mean_load), [])
        # Deduct assigned work from the main workload
        work = deduct(split, work)
        # Store assigned work
        allotted.append(split)
        if len(allotted) == across:
            break

    # Randomly assign remaining work
    for pending in work:
        idx = random.randint(0, across - 1)
        aw = allotted[idx]
        for (i, ele) in enumerate(aw):
            if ele[0] == pending[0]:
                aw[i] = (ele[0], ele[1] + pending[1], ele[2])
                break
        else:
            aw.append(pending)
        allotted[idx] = aw

    return allotted


def distribute_work(conf, toolchains):
    logger.info('distributing work across toolchains')
    conf = parse_content(conf)
    prog_tuples = extract_prog_tuples(conf)
    work_splits = split_work(prog_tuples, len(toolchains))
    assigned = {}
    for (tc, split) in zip(toolchains, work_splits):
        assigned[tc] = split

    return assigned
