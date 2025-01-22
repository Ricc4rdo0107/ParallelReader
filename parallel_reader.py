import os
from math import ceil
from multiprocessing import Process, Manager

class BigFileLoader:
    def __init__(self, filename: str):
        assert os.path.isfile(filename), f"File does not exists or its a directory"
        self.filename = filename
        self.size = os.path.getsize(filename)
    
    def _process_chunk(self, start, end, manager, idx):
        with open(self.filename, "r", newline="") as fi: #this shit(fuck microsoft)
            fi.seek(start)
            new_content = fi.read(end-start)
            manager.append((idx, new_content))

    def get_content(self, num_chunks: int) -> str:
        chunks_size = ceil(self.size / num_chunks)
        chunks = []

        print(f"File lenght: {self.size}")
        chunks = [ x for x in range(0, self.size, chunks_size) ]
        chunks_ranges = list(map(lambda x: (x, min(x + chunks_size, self.size)), chunks))
        content = Manager().list()
        processes: list[Process] = []

        for idx, (i1, i2) in enumerate(chunks_ranges):
            new_process = Process(target=self._process_chunk, args=(i1, i2, content, idx))
            processes.append(new_process)
            print(f"Process {idx} created with {i1} {i2}")
        
        for process in processes:
            process.start()
        for process in processes:
            process.join()
        
        print(list(sorted(content, key=lambda x:x[0])))

        return "".join(x for _, x, in sorted(content, key=lambda x:x[0]))
