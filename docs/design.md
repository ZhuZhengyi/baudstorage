# key design points of BaudStorage

## Append

1, consistency

the leader locks the extent to avoid concurrent append operations.

'commit length' = the minimum size of the three extent replicas; moreover, it is remembered by the extent leader. 

2, performance

the client firstly writes to the extent and then to the inode - double write overhead. 

observation: actually we don't need absolutely accurate file size. 

optimization: synchronously update inode when sealing the last extent and creating a new extent; asynchronously update file size when appending the current extent. 



