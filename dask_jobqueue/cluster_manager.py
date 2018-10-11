import logging
import threading

from distributed.deploy import Cluster

from distributed.utils import log_errors

logger = logging.getLogger(__name__)


class ClusterManager(Cluster):
    """ Intermediate Cluster object that should lead to a real ClusterManager

    This tries to improve upstream Cluster object and underlines needs for
    better decoupling between ClusterManager and Scheduler object
    """

    def __init__(self):
        self._target_scale = 0
        self._lock = threading.Lock()

    def _scale(self, n):
        """ Asynchronously called scale method

        This allows to do every operation with a coherent ocntext
        """
        with log_errors(), self._lock:
            # here we rely on a ClusterManager attribute to retrieve the
            # active and pending workers
            if n == self._target_scale:
                pass
            elif n > self._target_scale:
                self.scale_up(n)
            else:
                to_close = self.scheduler.workers_to_close(
                    n=len(self.scheduler.workers) - n)
                logger.debug("Closing workers: %s", to_close)
                self.scheduler.retire_workers(workers=to_close)
                # To close may be empty if just asking to remove pending
                # workers, so we should also give a target number
                self.scale_down(n, to_close)
            self._target_scale = n

    def scale(self, n):
        """ Scale cluster to n workers

        Parameters
        ----------
        n: int
            Target number of workers

        Example
        -------
        >>> cluster.scale(10)  # scale cluster to ten workers

        See Also
        --------
        Cluster.scale_up
        Cluster.scale_down
        """
        # TODO we should not rely on scheduler loop here, self should have its
        # own loop
        self.scheduler.loop.add_callback(self._scale, n)
