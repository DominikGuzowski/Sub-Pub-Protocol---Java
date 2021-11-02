# Computer Networks Protocol - Build Instructions

* Make is required.
* Without make, inspect the Makefile to see how the files are to be compiled and ran with specific inputs.

### Step 1
Extract zip contents into a folder.

### Step 2
When on the same level as the Protocol folder and src folder do:
1. To run a broker:
```
make broker port=??? brokerip=???
```
2. To run an actuator:
```
make actuator port=??? brokerip=??? topic=??? freq=???
```
3. To run the dashboard:
```
make dashboard port=??? brokerip=???
```

###### For more detailed description see Makefile.