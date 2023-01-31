import multiprocessing
from multiprocessing import (
    Pool,                               # to run multiple process
    Process,                            # to start a single process
    Manager,                            # to share memory between processes
)

def array_split(array: list, nsplits: int):
    """_summary_

    Args:
        array (list): _description_
        nsplits (int): _description_

    Returns:
        _type_: _description_
    """
    from math import floor
    lines_per_sub_array = len(array) / nsplits
    sub_arrays = []
    for i in range(nsplits):
        starting_point  = floor(i*lines_per_sub_array)
        ending_point    = floor((i+1)*lines_per_sub_array) 
        sub_arrays.append(array[starting_point:ending_point])
    return sub_arrays
        
def forking(func: callable, array: list, num_process: int = 2, 
            batch: int = 1, shares: Iterable = []):
    """Forking() helps simplify the forking processes. Very similar to PoolMap

    Args:
        func (callable): function of each process
        array (list): inputs for the functions
        num_process (int, optional): number of prcesses. Defaults to 2.
        batch (int, optional): the size of each batch. Defaults to 1.
        shares ()
    """
    
    # validating inputs
    if num_process < 1:
        raise Exception('Numer of processes must be at least 1')
    
    if batch < 1:
        raise Exception('The size of a batch must be at least 1')
        
    # manager can be slow
    manager = Manager()
    shared_list = []
    for share_item in shares:
        if isinstance(share_item, dict) or isinstance(share_item, set):
            _dict = manager.dict()
            _dict = deepcopy(share_item)
            shared_list.append(_dict)
        elif isinstance(share_item, list):
            _list = manager.list()
            _list = deepcopy(share_item)
            shared_list.append(_list)
        elif isinstance(share_item, int):
            # manager.Value
            shared_list.append(multiprocessing.Value('i',share_item))
        elif isinstance(share_item, float):
            shared_list.append(multiprocessing.Value('d',share_item))
        else:
            raise Exception(f'Sorry, the value type {type(share_item)} is currently not supported!')
    # clear up memories
    del shares
    
    # worker function
    def worker_fn(_batches: list):
        for bat in _batches:
            func(bat, *shared_list)
    
    sub_processes = array_split(array = array, nsplits = num_process)
    workers = []
    
    # for each process
    for sub_process in sub_processes:
        # divide subprocess into batches
        batches = [sub_process[i : i+batch] for i in range(0, len(sub_process), batch)] if batch > 1 else sub_process
        # create worker process
        workers.append(Process(target=worker_fn, args=(batches,)))
        # run worker process
        workers[-1].start()

    for idx in range(len(workers)):
        # wait for work process
        workers[idx].join()
        
    # return the shared variables as a list     
    return [item for item in shared_list]
