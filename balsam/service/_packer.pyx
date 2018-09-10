import random
import numpy as np
import cython

@cython.boundscheck(False)
@cython.wraparound(False)
cdef int any_occupied(int [:,:] a, int x1, int x2, int y1, int y2):
    cdef int ix, iy
    for ix in range(x1, x2+1):
        for iy in range(y1, y2+1):
            if a[ix, iy]: return 1
    return 0

@cython.boundscheck(False)
@cython.wraparound(False)
cdef int bisect_left(double [:] a, int n,  double x):
    cdef Py_ssize_t lo, hi, mid
    lo = 0
    hi = n
    while lo < hi:
        mid = (lo+hi)//2
        if a[mid] < x: lo = mid+1
        else: hi = mid
    return lo

class Rect:
    def __init__(self, xdim, ydim, id):
        self.xdim = xdim
        self.ydim = ydim
        self.id = id
    def __repr__(self):
        return f'<Rect{self.id}: ({self.xdim}, {self.ydim})>'

class BinPacker:
    def __init__(self, max_x, max_y):
        self.splits_x = np.array([0, max_x], dtype=np.float64)
        self.splits_y = np.array([0, max_y], dtype=np.float64)
        self.max_x = max_x
        self.max_y = max_y
        self.grid = np.zeros((1,1), dtype=np.int32)
        self.placed_rects = []

    def add_rect(self, int ix, int iy, double split_x, double split_y, int br_ix, int br_iy):
        if split_x not in self.splits_x:
            self.splits_x = np.insert(self.splits_x, br_ix+1, split_x)
            self.grid = np.insert(self.grid, br_ix, self.grid[br_ix,:], axis=0)
        if split_y not in self.splits_y:
            self.splits_y = np.insert(self.splits_y, br_iy+1, split_y)
            self.grid = np.insert(self.grid, br_iy, self.grid[:,br_iy], axis=1)
        self.grid[ix:br_ix+1, iy:br_iy+1] = 1

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def try_place(self, rect, first_col=False):
        fit = None
        cdef int [:,:] grid
        cdef int ix, iy, ixmax, iymax, br_ix, br_iy, i, j
        cdef double xdim, ydim, max_x, max_y
        cdef double ul_x, ul_y, br_x, br_y
        cdef double [:] splits_x
        cdef double [:] splits_y
        cdef int nsplits_x, nsplits_y

        splits_x = self.splits_x
        splits_y = self.splits_y
        nsplits_x = len(splits_x)
        nsplits_y = len(splits_y) 
        xdim = rect.xdim
        ydim = rect.ydim
        max_x = self.max_x
        max_y = self.max_y
        grid = self.grid
        ixmax, iymax = self.grid.shape
        if first_col: iymax = 1
        for iy in range(0, iymax):
            for ix in range(0, ixmax):
                if grid[ix, iy]: continue
                ul_x = splits_x[ix]
                ul_y = splits_y[iy]
                br_x = ul_x + xdim
                br_y = ul_y + ydim
                if br_x > max_x or br_y > max_y: continue
                br_ix = bisect_left(splits_x, nsplits_x, br_x) - 1 # using custom-C bisect is performance-critical
                br_iy = bisect_left(splits_y, nsplits_y, br_y) - 1
                if not any_occupied(grid, ix, br_ix, iy, br_iy):
                    self.add_rect(ix, iy, br_x, br_y, br_ix, br_iy)
                    ul = self.splits_x[ix], self.splits_y[iy]
                    self.placed_rects.append((ul, rect))
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

def main_draw(*, num_rect=500, xmax=2048, ymax=1440, rect_xrange=(16,128), rect_yrange=(30,100), draw_interval=100):
    packer = BinPacker(xmax, ymax)
    rects = [Rect(random.randint(*rect_xrange), 
                  random.randint(*rect_yrange), 
                  i) 
             for i in range(num_rect)]

    for tot, r in enumerate(sorted(rects, key = lambda r: r.xdim, reverse=True), 1): 
        packer.try_place(r)
        if tot % draw_interval == 0: 
            packer.report(draw=True)
