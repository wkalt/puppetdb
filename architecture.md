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

### Rules

Rules classify nodes into groups. Rule inputs are node properties. This
includes the node name, facts, and groups from other rules. Rules use an
s-expression-like language to specify matches against these properties.


### Groups

Groups are a way to organize nodes. Nodes are classified into groups using
rules, and those groups specify properties to be assigned to the nodes. Those
properties are:

 * Classes - Puppet classes to be included on nodes
 * Class parameters - Parameters to the classes
 * Variables - Top scope bindings that will be available within the nodes
   manifests
 * Environment - The puppet environment to use when compiling catalogs for the
   nodes
 * ACLs - It will be possible to restrict group visibility and modification to
   specific roles.


### Classes

Classes are puppet classes defined in manifests and modules. We store a list
of the classes we know about. They can be manually input or populated by
querying the puppet master. We will need to track the difference so we don't
remove manually added classes when doing automated updates. A class has an
environment, and a list of parameters with default values.


### Nodes

We track a list of nodes that we know about, to aid filling in data and
provide error checking.


### Environments

Puppet environments specify the list of available modules and manifests. We
store a list of environments that we know about.

