1. Flush SD card with the newest Lite OS.
2. Touch /boot/ssh on the pi.
3. Connect pi to the Internet router via a network cable.
4. Connect dev machine to same router.
5. ssh pi@`the ip address` with password 'raspberry'. 
   1. Change the password. 
   2. Change hostname to pi-`nextname`.
   3. Connect the pi to the Raft router via wireless.
   4. Run `sudo apt-get update`.
   5. Reboot.
6. Run `ssh-keygen -t ed25519 -f .ssh/pi-nextname`
7. Copy SSH key pub file to pi.
   1. `mkdir -p .ssh`
   2. `mv pi.pub .ssh/authorized_keys`
