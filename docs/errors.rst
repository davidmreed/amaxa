Error Behavior and Recovery
---------------------------

Because error recovery when loading complex object networks can be challenging and the overall load operation is not atomic, it's strongly recommended that all triggers, workflow rules, processes, validation rules, and lookup field filters be deactivated during an Amaxa load process. It's far easier to prevent errors than to fix them.

Amaxa executes loads in two stages, called *inserts* and *dependents*. In the *inserts* phase, Amaxa loads records of each sObject in sequence. In the *dependents* phase, Amaxa runs updates to populate self-lookups and dependent lookups on the created records. In both phases, Amaxa stops loading data when it receives an error from Salesforce. Since Amaxa uses the Bulk API, the stoppage occurs at the end of the sObject and phase that's currently processing.

If Accounts, Contacts, and Opportunities are being loaded, and an error occurs during the insert of Contacts, Amaxa will stop at the end of the Contact insert phase. All successfully loaded Accounts and Contacts remain in Salesforce, but no work is done for the *dependents* phase. If the error occurs during the *dependents* phase, all records of all sObjects have been loaded, but dependent and self-lookups for the errored sObject and all sObjects later in the operation are not populated.

Details of the errors encountered are shown in the results file for the errored sObject, which by default is ``sObjectName-results.csv`` but can be overridden in the operation definition.

Attempting Recovery
*******************

When Amaxa stops due to errors, it saves a *state file*, which preserves the phase and progress of the load operation. The state file for some operation ``operation.yaml`` will be called ``operation.state.yaml``. The state file persists the map of old to new Salesforce Ids that were successfully loaded, as well as the position the operation was in when the failure occured.

Should a failure occur, you can take action to remediate the failure, including making changes to the records in your ``.csv`` files or altering the metadata in your org. You can then resume the load by repeating your original command and adding the ``-s <state-file>`` option:

.. code-block:: shell

    $ amaxa --load operation.yaml -c credentials.yaml -s operation.state.yaml

Amaxa will pick up where it left off, loading only the records which failed or which weren't loaded the first time. (You may add records to the operation, in any sObject, and Amaxa will pick them up upon resume provided that the original failure was in the *inserts* phase - do not add new records if Amaxa has reached the *dependents* phase). It will also complete any un-executed passes to populate dependent and self-lookups.
