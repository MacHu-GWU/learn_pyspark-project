
.. image:: https://readthedocs.org/projects/learn_pyspark/badge/?version=latest
    :target: https://learn_pyspark.readthedocs.io/README.html
    :alt: Documentation Status

Welcome to ``learn_pyspark`` Documentation
==============================================================================
本项目是我个人学习 PySpark 时的笔记. 该项目被部署在了 `Readthedocs.org <https://learn_pyspark.readthedocs.io/README.html>`_ 上以供随时查看.

最方便的学习 PySpark 的方式是使用 PySpark 官方的 Docker Image, 然后再本地使用镜像以及使用 Jupyter Notebook 进行开发. PySpark 官方有很多 Docker Image, 注意你要选预装了 `jupyter 和 pyspark <https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook>`_ 的这个.

并且你要注意选对镜像的 `Tag <https://hub.docker.com/r/jupyter/pyspark-notebook/tags?page=1>`_. 如果你用的是 Arm 架构的 Mac, 你就要选择支持 Arm 架构的 Tag.

这里我提供了一个 `Shell Script <./bin/run_pyspark_container.sh>`_ 来启动这个 Container. 我使用的是 MacOS Arm 芯片的 Macbook Pro Max. 记得安装并启动 `Docker desktop <https://www.docker.com/products/docker-desktop/>`_. 如果你是有经验的 container 用户, 用别的 container engine 比如 `podman <https://podman.io/>`_ 也是可以的.
