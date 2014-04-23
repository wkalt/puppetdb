Architecture
========


Interaction with Puppet
-----------------------


       +---------------------+
       |      Node           |
       |---------------------|         +---------------------+
       |   +----------+      |         |                     |
       |   |  Facter  |      |         |                     |
       |   |          |      |         |      Puppet         |
       |   |  +------------+ |  1      |      master         |
       |   |  |   Puppet   +---------> |                     |
       |   +--|   agent    | |   4     |                     |
       |      +------------+ <---------+                     |
       +---------------------+         +------+--------------+
                                              |     ^
                                              |     |
                                             2|     |3
                                              |     |
                                              v     |
                                       +------------+--------+
                                       |                     |
                                       |     Node            |
                                       |     classifier      |
                                       |                     |
                                       +---------------------+

 1. The puppet agent requests a catalog from the puppet master,
    submitting its facts.

 2. The puppet master requests a node object from the node classifier,
    passing along facts.

 3. The node classifier applies rules based on the submitted data, and
    returns a resulting node object to the puppet master.

 4. The puppet master uses the node information to compile a catalog and
    returns this to the puppet agent.


Data stored in classifier
-----------------------

### Groups

Groups are a way to organize nodes. Nodes are classified into groups using
rules, and those groups specify properties to be assigned to the nodes. Those
properties are:

 * Classes - Puppet classes to be included on nodes
 * Class parameters - Parameters to the classes
 * Variables - Top-scope bindings that will be available within the nodes
   manifests
 * Environment - The puppet environment to use when compiling catalogs for the
   nodes
 * Rules - The rules that classify nodes into this group.


### Rules

Rules classify nodes into groups. Rule inputs are node properties. This
includes the node name, facts, and trusted data. Rules use an
s-expression-like language to specify matches against these properties.


### Classes

Classes are puppet classes defined in manifests and modules.  We store a
list of the classes we know about. This list is populated by querying
the puppet master. Each class has a list of parameters with default
values. Classes and their parameters are contained in environments (the
environment where the puppet code for that class exists).


### Environments

Puppet environments encapsulate sets of available modules and manifests
(for our purposes classes and class parameters). We store a list of
environments that we know about. Environments and groups can both be
seen as sets containing nodes, but environments are an outcome of
classification and are used to separate different versions of code
available for compilation whereas groups are categories used during the
process of classification in order to group classification outcomes.
