import random
import numpy as np
import bisect
import time
import cProfile

class Rect:

    CURRENT_ID = 0

    def __init__(self, xdim, ydim, id=None):
        self.xdim = xdim
        self.ydim = ydim
        if id is not None: 
            self.id = id
        else:
            self.id = Rect.CURRENT_ID
            Rect.CURRENT_ID += 1

    def __repr__(self):
        return f'<Rect{self.id}: ({self.xdim}, {self.ydim})>'

    @classmethod
    def rand_rect(cls, width_range, height_range):
        w = random.randint(*width_range)
        h = random.randint(*height_range)
        rect = cls(w, h, cls.CURRENT_ID)
        cls.CURRENT_ID += 1
        return rect

class BinPacker:
    def __init__(self, max_x, max_y):
        self.splits_x = [0, max_x]
        self.splits_y = [0, max_y]
        self.max_x = max_x
        self.max_y = max_y
        self.grid = np.zeros((1,1), dtype=np.bool)
        self.placed_rects = []

    def empty_iter(self): 
        return sorted(np.argwhere(self.grid == False), key= lambda x: (x[1],x[0]))

    def check_fit(self, rect, ix, iy):
        ul = self.splits_x[ix], self.splits_y[iy]
        br = ul[0] + rect.xdim, ul[1] + rect.ydim
        if br[0] > self.max_x or br[1] > self.max_y: return False

        br_ix = bisect.bisect_left(self.splits_x, br[0]) - 1
        br_iy = bisect.bisect_left(self.splits_y, br[1]) - 1
        
        if np.any(self.grid[ix:br_ix+1, iy:br_iy+1]): return False
        return br, (br_ix, br_iy)

    def add_rect(self, ix, iy, split_x, split_y, br_ix, br_iy):
        if split_x not in self.splits_x:
            self.splits_x.insert(br_ix+1, split_x)
            self.grid = np.insert(self.grid, br_ix, self.grid[br_ix,:], axis=0)
        if split_y not in self.splits_y:
            self.splits_y.insert(br_iy+1, split_y)
            self.grid = np.insert(self.grid, br_iy, self.grid[:,br_iy], axis=1)
        self.grid[ix:br_ix+1, iy:br_iy+1] = True

    def try_place(self, rect, first_col=False):
        fit = None
        for ix, iy in self.empty_iter():
            if first_col and iy > 0: break
            fit = self.check_fit(rect, ix, iy)
            if fit: break
        if fit:
            (split_x, split_y), (br_ix, br_iy) = fit
            self.add_rect(ix, iy, split_x, split_y, br_ix, br_iy)
            ul = self.splits_x[ix], self.splits_y[iy]
            self.placed_rects.append((ul, rect))
        else:
            #print(rect, 'did not fit')
            return

    def shrink_x_to_fit(self): 
        occ_x, occ_y = np.where(self.grid)
        x_bound = self.splits_x[occ_x.max()+1]
        self.max_x = x_bound
        self.splits_x[-1] = self.max_x
    
    def shrink_y_to_fit(self): 
        occ_x, occ_y = np.where(self.grid)
        y_bound = self.splits_y[occ_y.max()+1]
        self.max_y = y_bound
        self.splits_y[-1] = self.max_y

    def report(self, draw=False):
        for ul, rect in self.placed_rects:
            print(rect, 'at', ul)
        if draw:
            self.draw()

    def draw(self):
        from matplotlib import pyplot as plt
        import matplotlib.patches as patches
        import matplotlib.cm as cm
        fig, ax = plt.subplots(1)

        colors = cm.rainbow(np.linspace(0,1, len(self.placed_rects)))
        title = f'Fit {len(self.placed_rects)} rects in bbox {self.max_x}x{self.max_y}'
        for c, (ul, rect) in zip(colors, self.placed_rects):
            ll = ul[0] + rect.xdim, ul[1]
            x, y = ll[1], self.max_x - ll[0]
            width, height = rect.ydim, rect.xdim
            r = patches.Rectangle((x,y), width, height, lw=1,
                                  edgecolor='k',alpha=0.5, facecolor=c)
            ax.add_patch(r)
            #ax.text(x+width/2, y+height/2, f'{rect.id}', fontsize=6)
        r = patches.Rectangle((0,0), self.max_y, self.max_x, lw=2, edgecolor='r', facecolor='none')
        ax.add_patch(r)
        ax.set_ylim(0, self.max_x)
        ax.set_xlim(0, self.max_y)
        ax.set_title(title)
        plt.show()

def main_draw():
    NUM_RECT = 5000
    XMAX, YMAX = 2048, 1440
    RECT_XRANGE = (2, 128)
    RECT_YRANGE = (30, 180)
    DRAW_INTERVAL = 200
    packer = BinPacker(XMAX, YMAX)
    rects = [Rect.rand_rect(RECT_XRANGE, RECT_YRANGE) for i in range(NUM_RECT)]

    start = time.time()
    try:
        for tot, r in enumerate(sorted(rects, key = lambda r: r.xdim, reverse=True), 1): 
            packer.try_place(r)
            if tot % DRAW_INTERVAL == 0: packer.report(draw=True)
            print(tot)
    except KeyboardInterrupt:
        elapsed = time.time() - start
        print(f'packed {tot} in {elapsed} seconds')
        packer.report(draw=True)

def main_prof():
    NUM_RECT = 600
    XMAX, YMAX = 2048, 1440
    RECT_XRANGE = (2, 128)
    RECT_YRANGE = (30, 180)
    packer = BinPacker(XMAX, YMAX)
    rects = [Rect.rand_rect(RECT_XRANGE, RECT_YRANGE) for i in range(NUM_RECT)]
    for r in sorted(rects, key = lambda r: r.xdim, reverse=True): 
        packer.try_place(r)

def pack():
    packer = BinPacker(10, 10)
    rects = [Rect(2+random.randint(-1,3), 2+random.randint(-2,4)) for i in range(4)]
    for r in sorted(rects, key = lambda r: r.xdim, reverse=True):
        packer.try_place(r, first_col=True)
    packer.report(draw=True)
    packer.shrink_x_to_fit()
    packer.report(draw=True)
    packer.shrink_y_to_fit()
    packer.report(draw=True)

cProfile.run('main_prof()', sort='cumtime')
#main_draw()
