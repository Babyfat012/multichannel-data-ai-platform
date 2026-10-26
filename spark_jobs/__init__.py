"""PLATFORM Spark jobs by layer; shared helpers live in spark_jobs.lib.

Run entrypoints with spark-submit and expose the repository parent on PYTHONPATH.
Python RDD jobs distribute the complete spark_jobs package to executors as a zip.
"""
