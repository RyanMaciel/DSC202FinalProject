## Set up CLI:

```
pip install databricks-cli
databricks configure
```
Then enter the username and password for databricks. The host is `https://dbc-a40ffe6e-d726.cloud.databricks.com/`

## Github Workflow
### Initial Setup
`git clone https://github.com/RyanMaciel/DSC202FinalProject.git`

You can also use ssh for connecting to github which doesn't prompt you for username/password every time. Learn more [here](https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh).

### Normal work
Pull from origin/master:
```
git pull origin master
```
Move into new branch:
```
git checkout -b "branch_name"
```
Upload the changes from master to databricks. Make sure you are in the flight_delay dir locally:
```
databricks workspace import_dir -o . /Users/<your_email@u.rochester.edu>/flight_delay
```

From here make your changes in databricks. Then pull back down from databricks:
```
databricks workspace export_dir -o /Users/<your_email@u.rochester.edu>/flight_delay/ .
```

Now the changes are local. Before we commit them, we have to stage them. Do this with:
```
git add -A
```

Now we can commit the changes, do:
```
git commit -m "<commit message describing what you did>"
```
I usually like commiting pretty frequently, usually after I finish any kind of discrete chunk of code that does something on its own. This way, if you mess up you can always revert back to a good state. It also can make it clearer to the team your process.

Now you can push up to github:
```
git push origin <branch_name>
```
Now your changes are on the `<branch_name>` branch on GitHub. From here create a pull request into master that describes what you did and maybe ping people in the chat to see if they can review before you merge. Once you've pulled into master you can delete the branch in Github interface. You can also delete locally like this:
```
git checkout master
git branch -d <branch_name>
```

Now get what's currently in master:
```
git pull origin master
```
And repeat!

Also, its important to note that you don't always need to be up to date on the changes in master. If you are working on a branch that doesn't really have anything to do with new changes in the master, you can keep working on what you're working on.
