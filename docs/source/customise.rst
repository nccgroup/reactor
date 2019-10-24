Customise Reactor
*****************

Reactor allows for the customisation of Rules, Loaders, Alerters, Enhancements, Plugins, and Notifiers. This instills
great flexibility in how Reactor can be used. For further information about how to extend Reactor see the below
tutorials which provide a step-by-step guide:

.. toctree::
    :maxdepth: 1

    customise/adding_rules
    customise/adding_loaders
    customise/adding_alerters
    customise/adding_enhancements
    customise/adding_plugins
    customise/adding_notifiers


.. _customise_prerequisites:

Prerequisites
^^^^^^^^^^^^^

These guides assume an ability to program in Python 3, basic ability to use the command line, and an understanding of
how :doc:`Reactor works <introduction>`

For all of these guides you will need to create a modules folder in the base Reactor folder:

.. code-block:: console

    $ mkdir my_modules
    $ cd my_modules
    $ touch __init__.py
