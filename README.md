# Belgian KBO data model

This repository contains the data model for the Belgian KBO 
(Kruispuntbank van Ondernemingen) as a set of CSV files. 
The data model is based on the 
[official documentation](https://economie.fgov.be/sites/default/files/Files/Entreprises/KBO/Cookbook-KBO-Open-Data.pdf) 
of the KBO.

The data itself can be downloaded after creating an account on 
[their website](https://kbopub.economie.fgov.be/kbo-open-data/login)

In the solution there is a CLI to import the downloaded data (CSV files) into a SQLite database, and an sample API to fetch company information based on an enterprise number (kbo nr).

# Import

To use the import tool:

* download & unzip the data from KBO
* run the CLI with --help to see the options

This will import Meta, Code, Enterprise, Establishment, Branch, Address, Contact and Denomination

(Note: activities are not used)

# API

The API project provides a /companies/{id} api that queries the database for company info.
