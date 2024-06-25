import { Feature } from './Feature';

export class Layer {
    name: string;
    version: number;
    extent: number;
    ids: BigInt64Array | Int32Array;
    geometries;
    properties;
    length: number;
    vals: [string, any][];

    constructor(name: string, version : number, extent: number, ids: BigInt64Array | Int32Array, geometries, properties, numFeatures: number) {
        this.name = name;
        this.version = version;
        this.extent = extent;
        this.ids = ids;
        this.geometries = geometries;
        this.length = numFeatures;
        this.properties = properties;
    }

    public feature = (i) => {
        if (i < 0 || i >= this.length) throw new Error('feature index out of bounds');
        /* eslint-disable @typescript-eslint/no-explicit-any */
        const p: { [key: string]: any } = {};
        for (const key in this.properties) {
            const value = this.properties[key];
            const val = value && value[i];
            if (val) {
                p[key] = val;
            }
        }
        return new Feature(this.ids[i], this.extent, this.geometries[i], p)
    }
}

