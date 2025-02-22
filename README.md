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

The API project provides two operations:
* GET /companies/{id}[?language=[lang]]
  retrieves company info by KBO nr (in any valid 10-digit format), if it exists
  if no company is found with that Id, a 400 Not Found is returned.
  * id: kbo nr
  * language (optional): language to return descriptions (e.g. 'name type'). Currently only nl  or fr, and in some cases de are used by the KBO data set.
* GET /companies?name=[name]&street=[street]&houseNumber=[houseNumber]&postalCode=[postalCode]&city=[city]&skip=[skip]&take=[take]&language=[lang]
  This returns a list of maximum 25 companies satisfying the criteria.
  Parameters:
  * name: company name should contain the given name (case insensitive)
  * street: street of any address (main/establishment/branch) should contain the given street (case insensitive)
  * houseNumber: house number of any address (main/establishment/branch) should be equal to this value
  * postalCode: postal code of any address (main/establishment/branch) should be equal to this value
  * city: city of any address (main/establishment/branch) should contain the given street (case insensitive)
  * skip (optional): skip the first [skip] results. Default 0.
  * take (optional): return at most [take] results. Default 25, can not exceed 25.
  * language (optional): language to return descriptions (e.g. 'name type'). Currently only nl  or fr, and in some cases de are used by the KBO data set.
