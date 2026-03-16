alias test='python2 /full/path/to/test.py'

这一行只对当前 shell 有效，要永久生效可以写入

echo "alias test='python2 /full/path/to/test.py'" >> \~/.bashrc

source \~/.bashrc

之后同样可以直接：

test -j 6005
