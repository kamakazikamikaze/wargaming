======================================
Brute-force polling from WG's database
======================================

This is the source code used for pulling data from WG's database. You will need
to install the ``wotconsole`` package via ``pip``. This will also install all
dependencies.

Currently, you can run this via cli by typing the following:

`python src/getAllFinal.py config/yourconfig.json`

Just be sure to modify the example configuration to your liking. A sample SQL
database with all player information is included so you don't need to
brute-force player IDs. Please note that I will *not* be keeping this
up-to-date. It has been split into two 7z files in order to stay under the
100MB file-size limit.