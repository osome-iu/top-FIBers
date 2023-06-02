---
title: "System architecture"
last_modified: "2023-05-23"
---
> Last modified: {{ page.last_modified | date: "%Y-%m-%d"}}

### Data and analysis
All data and analysis are kept on the `lenny` machine in this repository:
```
/home/data/apps/topfibers/
```

Note that this projects [repository](https://github.com/osome-iu/top-FIBers) is cloned via `git` while _inside of the `topfibers/` directory_ and the name of the repository is specified as `repo`. 

E.g. via:
```
git clone git@github.com:osome-iu/top-FIBers repo
```

### Database and website
The database and the website code are kept on `lisa`. Once the database is updated the website (https://osome.iu.edu/tools/topfibers) automatically updates with the latest data. Find the front-end code [here](https://github.iu.edu/truthy-team/TopFIBers-dashboard).
