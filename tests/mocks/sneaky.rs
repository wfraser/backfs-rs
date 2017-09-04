// Sneaky Data Peeking and Manipulation for Tests
//
// Copyright (c) 2016 by William R. Fraser
//

use std::borrow::{Borrow, BorrowMut};
use std::cell::UnsafeCell;
use std::rc::Rc;

pub struct Sneaky<T> {
    inner: Rc<UnsafeCell<T>>
}

impl<T> Sneaky<T> {
    pub fn new(inner: T) -> Sneaky<T> {
        Sneaky {
            inner: Rc::new(UnsafeCell::new(inner))
        }
    }

    pub unsafe fn sneak(&mut self) -> Sneaky<T> {
        Sneaky {
            inner: self.inner.clone()
        }
    }
}

impl<T> Borrow<T> for Sneaky<T> {
    fn borrow(&self) -> &T {
        unsafe { &*self.inner.get() }
    }
}

impl<T> BorrowMut<T> for Sneaky<T> {
    fn borrow_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}
