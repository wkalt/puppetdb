# Classifier

A service for classifying puppet nodes based on user-defined rules.

# Getting Started

NB: This is in an early stage of development.
These instructions are really only useful if you want to develop on the classifier.

To run the classifier you will need:

* Postgres
* A JVM
* [Leiningen](http://leiningen.org)

First, create a fresh postgres database for the classifier.
This can be done as a postgres superuser (usually `su - postgres` on a default installation).
Here's an example of creating the user and database, both with a name of `classifier`:

```
createuser -d classifier -P
createdb classifier -U classifier
```

The first command will prompt for the user's password.

Edit (or copy to another location) the `ext/classifier.ini` configuration file and add a `database` section with the database name, user, and password.
For the example above, assuming `classifier` was entered for the user's password, this section should look like:

```ini
[database]
dbname = classifier
user = classifier
password = classifier
```

Now you can start the service with (use the path to the location of your ini file):

```
lein run --config ext/classifier.ini
```

Once the service is running, you can give it a rough smoke test by creating a simple group hierarchy and using it to classify a node.

First, add a class that you can reference in your groups.
Note that, when using the classifier with Puppet, all class definitions will be pulled from the master, but for this smoke test you'll use API routes to define the class.

```sh
curl -X PUT -H 'Content-Type: application/json' \
  -d '{ "name": "apache",
        "environment": "production",
        "parameters": {
          "confd_dir": "/etc/apache2",
          "mod_dir": "/etc/apache2/mods-available",
          "logroot": "/var/log/apache2"
        }
      }' \
  http://localhost:1261/v1/environments/production/classes/apache
```

If all went well, the output from curl should echo back the same class JSON object that you submitted.

Now, you can create a group that uses this class.

```sh
curl -X PUT -H 'Content-Type: application/json' \
  -d '{ "name": "webservers",
        "environment": "production",
        "parent": "default",
        "rule": {"when": ["~", "name", "\\.www\\.example\\.com$"]},
        "classes": {
          "apache": {
            "confd_dir": "/opt/mywebapp/etc/apache2",
            "logroot": "/opt/mywebapp/log"
          }
        }
      }' \
  http://localhost:1261/v1/groups/webservers
```

Again, if you see the group object that was submitted echoed back in curl's output, then the submission worked.

Now you can classify a node:

```sh
curl http://localhost:1261/v1/classified/nodes/argon.atlanta.www.example.com
```

The response should be:

```json
{
   "name" : "argon.atlanta.www.example.com",
   "parameters" : {
      "apache" : {
         "confd_dir": "/opt/mywebapp/etc/apache2",
         "logroot" : "/opt/mywebapp/log"
      }
   },
   "classes" : [
      "apache"
   ],
   "variables" : {},
   "groups" : [
      "webservers"
   ],
   "environment" : "production"
}
```

It worked!
As you can see, the node was classified into the `webservers` group that you created, and picked up the parameters that the group set for the `apache` class.
