from datetime import timedelta

TASK_QUEUE = "meeting-analysis"
TRANSCRIBER_TASK_QUEUE = "meeting-transcription"

# Heartbeat config kept in one place so the server-side timeout and the send interval stay
# consistent across workflows, activities and both workers. The interval is both the
# heartbeat cadence and the worker throttle interval, so the server is refreshed every
# ~INTERVAL, well inside TIMEOUT.
HEARTBEAT_TIMEOUT = timedelta(seconds=30)
HEARTBEAT_INTERVAL = timedelta(seconds=5)

# On SIGTERM (scale-down) the worker stops polling and drains its in-flight activities before
# exiting, so scale-down does not kill busy pods and orphan their work. This is a cap, not a
# fixed wait: the pod exits as soon as its activities finish. 15 min covers even the longest
# activity (~700s diarize); heartbeat recovery is the backstop for hard crashes. Pod
# terminationGracePeriodSeconds must be >= this.
GRACEFUL_SHUTDOWN = timedelta(minutes=15)
