## Job Engine Design

> Purpose: The **Job Engine** is a coordinator which manage the tasks' life cycle and CRUD of the tasks. To be clearified, the **Job Engine** does not run the task itself, instead the task runs on hadoop(or whatever it depends).

### Executable

![Class Diagram](Class_Diagram.png)

**Executable** is a top-level interface for all kinds of jobs or tasks.

**AbstractExecutable** is a abstract implementation of **Executable**, it provides:

 1. some getter and setter method
 2. default implementation of **Executable**.execute()
 3. life cycle method of an **Executable** and their default implementation

**DefaultChainedExecutable** is an implementation of AbstractExecutable which contains a group of **Executable**

### ExecutableManager
**ExecutableManager** provide the CRUD function for an **Executable**

### ExecutableDao
**ExecutableDao** provide the access of the persistent object for Executable

There are two persistent object for one **Executable**

1. **ExecutablePO** is to store the runnning parameters for the **Executable**, and once the **Executable** is submitted, **ExecutablePO** is unmodifiable.
2. **ExecutableOutputPO** is to store the running result for the **Executable**, for instance the current state, error log.

### DefaultScheduler
**DefaultScheduler** is a coordinator for **Executable**s.

There is a daemon thread call JobFetcher running periodically. It is responsible for scheduling the **Executable**s

    Note: there should always be only one instance running in the cluster. And it is configured using "kylin.server.mode" in the "kylin.properties", there are two modes "all" & "query", "all" means it will defaultly start the scheduler. So if there are multiple kylin instances, make sure there is only one instance whose "kylin.server.mode" is set to "all".

    