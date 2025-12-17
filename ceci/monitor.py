"""Tools for monitoring processing status and memory usage by ceci pipelines"""

import time
import psutil
import threading
import datetime


class MemoryMonitor:
    """
    A monitor which reports on memory usage by this process and all child processes
    throughout the lifetime of a process.

    The monitor is designed to be run in a thread, which is done automatically in the
    start_in_thread method, and will then continue until either the main thread ends
    or the stop method is called from another thread.

    To print out different process information you could use subclass and override the
    log method.
    """

    def __init__(self, interval=30):
        """Create a memory monitor.

        Parameters
        ----------
        interval: float or int
            The interval in seconds between each report.
            Default is 30 seconds
        """
        self.should_continue = True
        self.interval = interval
        self.process = psutil.Process()
        self.max_total_rss = 0.0  # Track maximum total RSS including children

    @classmethod
    def start_in_thread(cls, *args, **kwargs):
        """Create a new thread and run the memory monitor in it

        For parameters, see the init method; all arguments sent to this method are
        passed directly to it.

        Returns
        -------
        monitor: MemoryMonitor
            The monitor, already running in its own thread
        """
        monitor = cls(*args, **kwargs)
        thread = threading.Thread(target=monitor._run)
        thread.start()
        return monitor

    def stop(self):
        """Stop the monitor and report maximum total memory usage.

        The monitor will complete its current sleep interval and then end.
        Reports the maximum total RSS (including children) observed during monitoring.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.should_continue = False
        # Do one final check to capture any remaining child processes
        try:
            final_total = self.get_total_rss_gb(self.process)
            if final_total > self.max_total_rss:
                self.max_total_rss = final_total
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        
        # Report the maximum total memory observed
        print(
            f"MemoryMonitor: Maximum total memory (including children): "
            f"{self.max_total_rss:.3f} GB"
        )

    @staticmethod
    def get_total_rss_gb(p):
        """Get total RSS memory in GB for process and all its children (recursively).
        
        Parameters
        ----------
        p: Process
            A psutil process
            
        Returns
        -------
        total_rss_gb: float
            Total RSS memory in GB including all child processes
        """
        try:
            # Get memory for this process
            mem = p.memory_info()
            total_rss = mem.rss
            
            # Add memory from all child processes (recursively)
            try:
                for child in p.children(recursive=True):
                    try:
                        child_mem = child.memory_info()
                        total_rss += child_mem.rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # Child process may have terminated or we may not have access
                        continue
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # Process may have terminated or we may not have access
                pass
            
            return total_rss / 1e9  # Convert to GB
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0

    def log(self, p):
        """Print memory usage information to screen, including child processes.

        Parameters
        ----------
        p: Process
            A psutil process
        """
        mem = p.memory_info()
        # report time since start of process
        dt = datetime.timedelta(seconds=time.time() - p.create_time())

        # Memory for this process only
        rss = mem.rss / 1e9
        vms = mem.vms / 1e9
        
        # Total memory including all child processes
        total_rss = self.get_total_rss_gb(p)
        
        # Track maximum total RSS over time
        if total_rss > self.max_total_rss:
            self.max_total_rss = total_rss
        
        avail = psutil.virtual_memory().available / 1e9

        # For now I don't use the python logging mechanism, but
        # at some point should probably switch to that.
        print(
            f"MemoryMonitor Time: {dt}   Physical mem: {rss:.3f} GB   "
            f"Total (with children): {total_rss:.3f} GB   "
            f"Virtual mem: {vms:.3f} GB   "
            f"Available mem: {avail:.1f} GB"
        )

    def _run(self):
        # there are two ways to stop the monitor - it is automatically
        # ended if the main thread completes.  And it can be stopped
        # manually using the stop method.  Check for both these.
        while threading.main_thread().is_alive():
            if not self.should_continue:
                break
            self.log(self.process)
            time.sleep(self.interval)
        
        # Ensure we report max when thread ends naturally
        if self.max_total_rss > 0:
            print(
                f"MemoryMonitor: Maximum total memory (including children): "
                f"{self.max_total_rss:.3f} GB"
            )
