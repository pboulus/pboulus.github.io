cd /home/pboulus/pboulus.github.io/app/
git pull origin master
python3 update.py
git add -A 
git commit -m "Automatic data update"
git push origin master