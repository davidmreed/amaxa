Limitations, Known Issues, and Future Plans
-------------------------------------------

Issues and roadmap are tracked in `GitHub Issues <https://github.com/davidmreed/amaxa/issues>`_.

Limitations
***********

- Amaxa does not support import or export of compound fields (Addresses and Geolocations), but can import and export their component fields, such as ``MailingStreet``.
- Amaxa does not support Base64 binary-blob fields.

Objectives
**********

Future plans include:

- Improvements to efficiency in API use and memory consumption.
- More sophisticated handling of references to "metadata-ish" sObjects, like Users and Record Types.
- Support for importing data from external systems that does not have a Salesforce Id
  - Note that manually synthesizing Id values in input data is fine, provided they conform to the expected length and character content of Salesforce Ids.
- Recursive logic on extraction to handle outside references.
