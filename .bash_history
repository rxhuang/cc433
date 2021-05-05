ls
git init
git add Project4_3/
git config --global user.name angelo
git commit -m "First commit"
git remote add origin git@github.com:rxhuang/cc43.git
git push origin HEAD:master
git push -u origin main
git remote rm origin 
git remote add origin https://github.com/rxhuang/cc43.git
git push origin HEAD:master
ls
