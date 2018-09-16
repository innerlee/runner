cd /home/lizz/dev/pysc2-rl-killer
python main.py --gpu $GPU --work_dir /home/lizz/population --sz 32 --render 0 --vf_coef 0.25 --ent_coef 0.0001 --discount 0.99 --clip_grads 1 --optimizer adam --beta1 0.9 --beta2 0.999 --step_mul 8 --lr 0.00001 --steps 32
