Activealchemy - SQLAlchemy Wrapper
==================================

Activealchemy is a wrapper around SQLAlchemy.

How to use it
-------------

To use it simply import your activealchemy model  like so:

    >>> from myapp.models import *


Useful features
---------------

Activealchemy automatically create sessions for you and can also rollback and
retry on error.
For this you just have to surround your query with the `q()` utility available
in activealchemy.utils.retry.
You can use it as a function:

    >>> from activealchemy.utils.retry import q
    >>> from myapp.models import File
    >>> q(File.all) # Select all files
    >>> q(File.session.add, args=[File()], commit=True)

Or using the `with` statement:

    >>> from activealchemy.utils.retry import Q
    >>> with Q(File, max_retries=3, commit=True) as q:
    ...     q(File.session.add, args=[new_file])
    ...     do_stuff()
    ...     q(File.session.add, args=[related_file])

In the above example, both file or none will be inserted, meaning that if
do\_stuff() or the second insert fail by raising an exception, all queries in
the `with` block will be rollbacked.

For all arguments of `Q` or `q()` please refers to the provided docstrings.


