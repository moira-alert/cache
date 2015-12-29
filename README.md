# Cache

[![Documentation Status](https://readthedocs.org/projects/moira/badge/?version=latest)](http://moira.readthedocs.org/en/latest/?badge=latest) [![Build Status](https://travis-ci.org/moira-alert/cache.svg?branch=master)](https://travis-ci.org/moira-alert/cache) [![Coverage Status](https://coveralls.io/repos/moira-alert/cache/badge.svg?branch=master&service=github)](https://coveralls.io/github/moira-alert/cache?branch=master) [![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/moira-alert/moira?utm_source=badge&utm_medium=badge&utm_campaign=badge)


Code in this repository is a part of Moira monitoring application. Other parts are [Worker][worker], [Notifier][notifier] and [Web][web].

Cache is responsible for receiving metric stream, filtering it and saving data to Redis. Only metrics that match any of the configured triggers are saved.

Documentation for the entire Moira project is available on [Read the Docs][readthedocs] site.

If you have any questions, you can ask us on [Gitter][gitter].


[worker]: https://github.com/moira-alert/worker
[notifier]: https://github.com/moira-alert/notifier
[web]: https://github.com/moira-alert/web
[readthedocs]: http://moira.readthedocs.org
[gitter]: https://gitter.im/moira-alert/moira
